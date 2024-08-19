from typing import Any, List, Optional, Union

import numpy as np


def read_pdf(
    file: Union[str, bytes],
    scale: int = 2,
    rgb_mode: bool = True,
    password: Optional[str] = None,
    **kwargs: Any,
) -> List[np.ndarray]:
    """Read a PDF file and convert it into an image in numpy format

    Args:
    ----
        file: the path to the PDF file
        scale: rendering scale (1 corresponds to 72dpi)
        rgb_mode: if True, the output will be RGB, otherwise BGR
        password: a password to unlock the document, if encrypted
        **kwargs: additional parameters to :meth:`pypdfium2.PdfPage.render`

    Returns:
    -------
        the list of pages decoded as numpy ndarray of shape H x W x C
    """
    import pypdfium2 as pdfium

    # Rasterise pages to numpy ndarrays with pypdfium2
    pdf = pdfium.PdfDocument(file, password=password, autoclose=True)
    return [
        page.render(scale=scale, rev_byteorder=rgb_mode, **kwargs).to_numpy()
        for page in pdf
    ]
