def get_device():
    import torch

    """
    Returns the appropriate torch device based on the available hardware.
    Supports CUDA, ROCm, MPS (Apple Silicon), and CPU.
    """
    if torch.cuda.is_available():  # This covers both CUDA and ROCm
        return torch.device("cuda")
    elif torch.backends.mps.is_available():  # Apple Silicon (M1/M2)
        return torch.device("mps")
    else:
        return torch.device("cpu")


def clear_cache() -> None:
    """
    Clears the GPU cache if applicable. Supports CUDA and ROCm.
    For MPS (Apple Silicon) and CPU, no operation is needed.
    """
    import torch

    if torch.cuda.is_available():  # This covers both CUDA and ROCm
        return torch.cuda.empty_cache()
    # No need to clear cache for MPS or CPU as they handle memory differently
