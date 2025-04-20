import logging
import multiprocessing
import os
import queue
import signal
import sys
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from multiprocessing.connection import Connection
from typing import Any, Dict, List, Optional, Sequence, Type, Union

from inferio.types import PredictionInput
from inferio.utils import clean_dict
from panoptikon.signal_handler import register_child  # Ensure this is correctly imported

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class LoadMessage:
    command: str = "load"
    request_id: str = ""
    kwargs: Optional[Dict[str, Any]] = None


@dataclass
class PredictMessage:
    command: str = "predict"
    request_id: str = ""
    inputs: Sequence[dict] = ()  # Changed to dict for serialization


@dataclass
class UnloadMessage:
    command: str = "unload"
    request_id: str = ""


@dataclass
class ResponseMessage:
    request_id: Optional[str] = None
    outputs: Optional[Sequence[Any]] = None
    error: Optional[str] = None
    status: Optional[str] = None


class InferenceModel(ABC):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @abstractmethod
    def load(self) -> None:
        pass

    @abstractmethod
    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        pass

    @abstractmethod
    def unload(self) -> None:
        pass

    def __del__(self):
        self.unload()


class ProcessIsolatedInferenceModel(InferenceModel, ABC):
    @classmethod
    @abstractmethod
    def concrete_class(cls) -> Type["InferenceModel"]:
        """Return the concrete InferenceModel class to instantiate in the process."""
        pass

    def __init__(self, **kwargs: Any) -> None:
        super().__init__()
        self._kwargs: Dict[str, Any] = kwargs
        self._process: Optional[multiprocessing.Process] = None
        self._parent_conn, self._child_conn = multiprocessing.Pipe()
        self._response_handlers: Dict[str, queue.Queue] = {}
        self._listener_thread: threading.Thread = threading.Thread(
            target=self._listen_responses, daemon=True
        )
        self._listener_thread.start()
        logger.debug(
            f"{self.__class__.name()} - Initialized ProcessIsolatedInferenceModel with kwargs: {self._kwargs}"
        )

    @classmethod
    def name(cls) -> str:
        return cls.concrete_class().name()

    def load(self) -> None:
        if self._process is None or not self._process.is_alive():
            logger.debug(f"{self.name()} - Starting subprocess.")
            self._start_subprocess()
            assert self._process is not None, "Subprocess not started."
            logger.debug(
                f"{self.name()} - Started subprocess with PID {self._process.pid}"
            )
            # Generate a unique request_id for the load operation
            load_request_id = str(uuid.uuid4())
            # Send load command with kwargs and request_id
            load_msg = LoadMessage(
                request_id=load_request_id, kwargs=clean_dict(self._kwargs) if self._kwargs else None
            )
            self._parent_conn.send(asdict(load_msg))
            try:
                response = self._get_response(load_request_id)
                if response.status == "loaded":
                    logger.debug(f"{self.name()} - Model loaded successfully.")
                elif response.error:
                    logger.error(
                        f"{self.name()} - Error during load: {response.error}"
                    )
                    self._handle_subprocess_crash()
                    raise RuntimeError(response.error)
            except queue.Empty:
                logger.error(
                    f"{self.name()} - Timeout waiting for load response."
                )
                self._handle_subprocess_crash()
                raise RuntimeError("Timeout waiting for load response.")
            except Exception as e:
                logger.error(
                    f"{self.name()} - Exception during load: {e}",
                    exc_info=True,
                )
                self._handle_subprocess_crash()
                raise e

        else:
            logger.debug(f"{self.name()} - Subprocess already running.")

    def predict(self, inputs: Sequence[PredictionInput]) -> Sequence[Any]:
        if not self._process or not self._process.is_alive():
            logger.error(
                f"{self.name()} - Subprocess is not running. Reloading."
            )
            self.load()

        request_id = str(uuid.uuid4())
        predict_msg = PredictMessage(
            request_id=request_id, inputs=[asdict(i) for i in inputs]
        )
        self._parent_conn.send(asdict(predict_msg))
        logger.debug(
            f"{self.name()} - Sent predict request with ID {request_id}"
        )
        try:
            response = self._get_response(request_id)
            if response.error:
                logger.error(
                    f"{self.name()} - Prediction error for request {request_id}: {response.error}"
                )
                raise RuntimeError(response.error)
            if response.outputs is None:
                logger.error(
                    f"{self.name()} - Prediction response outputs are None for request {request_id}"
                )
                raise RuntimeError("Prediction response outputs are None.")
            logger.debug(
                f"{self.name()} - Received prediction for request {request_id}"
            )
            return response.outputs
        except queue.Empty:
            logger.error(
                f"{self.name()} - Timeout waiting for predict response for request ID {request_id}."
            )
            self._handle_subprocess_crash()
            raise RuntimeError("Timeout waiting for predict response.")
        except Exception as e:
            logger.error(
                f"{self.name()} - Exception during predict for request ID {request_id}: {e}",
                exc_info=True,
            )
            self._handle_subprocess_crash()
            raise

    def unload(self) -> None:
        if self._process is not None and self._process.is_alive():
            # Generate a unique request_id for the unload operation
            unload_request_id = str(uuid.uuid4())
            unload_msg = UnloadMessage(request_id=unload_request_id)
            self._parent_conn.send(asdict(unload_msg))
            logger.debug(f"{self.name()} - Sent unload command.")
            try:
                response = self._get_response(unload_request_id, timeout=10)
                if response.status == "unloaded":
                    logger.debug(
                        f"{self.name()} - Model unloaded successfully."
                    )
            except queue.Empty:
                logger.error(
                    f"{self.name()} - Timeout waiting for unload response."
                )
            except Exception as e:
                logger.error(f"{self.name()} - Error during unload: {e}")
            finally:
                self._process.join(timeout=5)
                if self._process.is_alive():
                    logger.error(
                        f"{self.name()} - Subprocess did not terminate, terminating forcefully."
                    )
                    self._process.terminate()
                    self._process.join(timeout=3)
                    if self._process.is_alive():
                        logger.error(
                            f"{self.name()} - Subprocess still alive, killing forcefully."
                        )
                        force_kill_process(self._process)
                        self._process.join(timeout=3)
                if self._process.is_alive():
                    logger.error(
                        f"{self.name()} - Subprocess still alive after force kill."
                    )
                self._parent_conn.close()
                self._process = None
        else:
            logger.debug(f"{self.name()} - Subprocess is not running.")

    @classmethod
    def _model_process(cls, conn: Connection, kwargs: Dict[str, Any]) -> None:
        """Run in the subprocess: instantiate and manage the concrete InferenceModel."""
        from dotenv import load_dotenv

        load_dotenv()
        from panoptikon.log import setup_logging
        setup_logging()
        try:
            model_class = cls.concrete_class()
            logger.debug(f"{model_class.name()} - Resolving concrete class.")
            model_instance = model_class(**kwargs)
            logger.debug(f"{model_class.name()} - Subprocess started.")
        except Exception as e:
            error_response = ResponseMessage(error=str(e))
            conn.send(asdict(error_response))
            logger.error(
                f"{cls.name()} - Error instantiating concrete class: {e}",
                exc_info=True,
            )
            conn.close()
            return

        try:
            while True:
                # Batch retrieval of all available messages
                messages = []
                while conn.poll():
                    try:
                        message = conn.recv()
                        messages.append(message)
                    except EOFError:
                        logger.error(
                            f"{model_class.name()} - Subprocess pipe closed."
                        )
                        break

                if not messages:
                    # If no messages are available, wait briefly to avoid busy waiting
                    threading.Event().wait(0.01)
                    continue

                predict_messages: List[PredictMessage] = []
                for message_dict in messages:
                    command = message_dict.get("command")
                    if command == "load":
                        load_msg = LoadMessage(**message_dict)
                        request_id = load_msg.request_id
                        try:
                            model_instance.load()
                            response = ResponseMessage(
                                request_id=request_id, status="loaded"
                            )
                            conn.send(asdict(response))
                            logger.debug(
                                f"{model_class.name()} - Model loaded in subprocess."
                            )
                        except Exception as e:
                            response = ResponseMessage(
                                request_id=request_id, error=str(e)
                            )
                            conn.send(asdict(response))
                            logger.error(
                                f"{model_class.name()} - Error loading model: {e}",
                                exc_info=True,
                            )
                    elif command == "predict":
                        predict_msg = PredictMessage(**message_dict)
                        predict_messages.append(predict_msg)
                    elif command == "unload":
                        # Before unloading, process any pending predict messages
                        if predict_messages:
                            cls._batch_predict(
                                conn, model_instance, predict_messages
                            )
                            # Clear the predict messages list
                            predict_messages.clear()

                        unload_msg = UnloadMessage(**message_dict)
                        request_id = unload_msg.request_id
                        try:
                            model_instance.unload()
                            response = ResponseMessage(
                                request_id=request_id, status="unloaded"
                            )
                            conn.send(asdict(response))
                            logger.debug(
                                f"{model_class.name()} - Model unloaded in subprocess."
                            )
                            break  # Exit the subprocess loop
                        except Exception as e:
                            response = ResponseMessage(
                                request_id=request_id, error=str(e)
                            )
                            conn.send(asdict(response))
                            logger.error(
                                f"{model_class.name()} - Error unloading model: {e}",
                                exc_info=True,
                            )
                if predict_messages:
                    cls._batch_predict(conn, model_instance, predict_messages)

        except Exception as e:
            error_response = ResponseMessage(error=str(e))
            conn.send(asdict(error_response))
            logger.error(
                f"{cls.name()} - Critical error in subprocess: {e}",
                exc_info=True,
            )
        finally:
            conn.close()
            logger.debug(f"{model_class.name()} - Subprocess terminating.")

    @staticmethod
    def _batch_predict(
        conn: Connection,
        model_instance: InferenceModel,
        predict_msgs: List[PredictMessage],
    ):
        MAX_BATCH_SIZE: int = int(os.getenv("MAX_COMBINED_BATCH", 32))

        batches: List[List[PredictMessage]] = []
        current_batch: List[PredictMessage] = []
        current_batch_size: int = 0

        for msg in predict_msgs:
            num_inputs = len(msg.inputs)
            if num_inputs > MAX_BATCH_SIZE:
                # If the message itself exceeds the batch size, process it individually
                if current_batch:
                    batches.append(current_batch)
                    current_batch = []
                    current_batch_size = 0
                batches.append([msg])
            else:
                if current_batch_size + num_inputs > MAX_BATCH_SIZE:
                    if current_batch:
                        batches.append(current_batch)
                    current_batch = [msg]
                    current_batch_size = num_inputs
                else:
                    current_batch.append(msg)
                    current_batch_size += num_inputs

        if current_batch:
            batches.append(current_batch)

        for batch in batches:
            combined_inputs = []

            for msg in batch:
                combined_inputs.extend(
                    [PredictionInput(**pi) for pi in msg.inputs]
                )

            if len(batch) > 1:
                logger.debug(
                    f"{model_instance.name()} - Performing batched prediction for {len(combined_inputs)} inputs."
                )
            try:
                # Perform batched prediction
                outputs = model_instance.predict(combined_inputs)

                # Split outputs back to individual requests
                start = 0
                for msg in batch:
                    end = start + len(msg.inputs)
                    individual_outputs = outputs[start:end]
                    response = ResponseMessage(
                        request_id=msg.request_id, outputs=individual_outputs
                    )
                    conn.send(asdict(response))
                    logger.debug(
                        f"{model_instance.name()} - Batched prediction completed for request {msg.request_id}."
                    )
                    start = end
            except Exception as batch_e:
                logger.error(
                    f"{model_instance.name()} - Batched prediction failed: {batch_e}. Processing individually.",
                    exc_info=True,
                )
                # If batch prediction fails, process each PredictMessage individually
                for msg in batch:
                    request_id = msg.request_id
                    try:
                        individual_inputs = [
                            PredictionInput(**pi) for pi in msg.inputs
                        ]
                        individual_outputs = model_instance.predict(
                            individual_inputs
                        )
                        response = ResponseMessage(
                            request_id=request_id, outputs=individual_outputs
                        )
                        conn.send(asdict(response))
                        logger.debug(
                            f"{model_instance.name()} - Individual prediction completed for request {request_id}."
                        )
                    except Exception as individual_e:
                        response = ResponseMessage(
                            request_id=request_id, error=str(individual_e)
                        )
                        conn.send(asdict(response))
                        logger.error(
                            f"{model_instance.name()} - Individual prediction error for request {request_id}: {individual_e}",
                            exc_info=True,
                        )

    def _listen_responses(self) -> None:
        while True:
            try:
                message_dict = self._parent_conn.recv()
                response = ResponseMessage(**message_dict)
                if response.request_id:
                    handler = self._response_handlers.get(response.request_id)
                    if handler:
                        handler.put(response)
                        logger.debug(
                            f"{self.name()} - Response mapped to request ID {response.request_id}."
                        )
                elif response.status:
                    # Handle status messages if needed
                    logger.debug(
                        f"{self.name()} - Received status message: {response.status}"
                    )
                elif response.error:
                    # Handle process-level errors if needed
                    logger.error(
                        f"{self.name()} - Received error message: {response.error}"
                    )
            except EOFError:
                logger.error(
                    f"{self.name()} - Subprocess pipe closed unexpectedly."
                )
                self._handle_subprocess_crash()
                break
            except Exception as e:
                logger.error(
                    f"{self.name()} - Error in response listener: {e}",
                    exc_info=True,
                )
                self._handle_subprocess_crash()
                break

    def _get_response(self, request_id: str, timeout: Optional[float] = None) -> ResponseMessage:
        response_queue: queue.Queue = queue.Queue()
        self._response_handlers[request_id] = response_queue
        logger.debug(
            f"{self.name()} - Waiting for response for request ID {request_id}."
        )
        try:
            response = response_queue.get(timeout=timeout)
            return response
        except queue.Empty:
            logger.error(
                f"{self.name()} - Timeout waiting for response for request ID {request_id}."
            )
            raise
        finally:
            if request_id in self._response_handlers:
                del self._response_handlers[request_id]

    def _start_subprocess(self) -> None:
        self._process = multiprocessing.Process(
            target=self._model_process,
            args=(self._child_conn, self._kwargs),
            daemon=True,
        )
        register_child(self._process)
        self._process.start()

    def _handle_subprocess_crash(self) -> None:
        logger.error(f"{self.name()} - Detected subprocess crash.")
        # Notify all pending requests about the crash
        for request_id, handler in self._response_handlers.items():
            error_response = ResponseMessage(
                request_id=request_id, error="Subprocess crashed unexpectedly."
            )
            try:
                handler.put(error_response)
            except Exception as e:
                logger.error(
                    f"{self.name()} - Failed to notify request ID {request_id}: {e}"
                )
        self._response_handlers.clear()
        # Clean up the process reference
        if self._process is not None:
            if self._process.is_alive():
                logger.debug(f"{self.name()} - Terminating subprocess.")
                self._process.terminate()
                self._process.join(timeout=3)
                if self._process.is_alive():
                    logger.debug(f"{self.name()} - Force killing subprocess.")
                    force_kill_process(self._process)
                    self._process.join(timeout=3)
            self._parent_conn.close()
            self._process = None

    def __del__(self):
        try:
            self.unload()
        except Exception as e:
            logger.error(f"{self.name()} - Exception during __del__: {e}")


def force_kill_process(process: multiprocessing.Process) -> None:
    if sys.platform == "win32":
        process.terminate()  # On Windows, this is equivalent to SIGTERM
    else:
        os.kill(process.pid, signal.SIGKILL)  # SIGKILL on Unix
