import pathlib

from io import BytesIO
from PIL import Image
from typing import Union
from ..fileinfo.fileinfo import FileInfo

def webpconvert(imagename: Union[pathlib.Path, str], compression_level: Union[int, str] = 80, lossless: bool = False, outname: Union[pathlib.Path, str] = None, verbose: bool = False):
    if type(imagename) != pathlib.Path:
        imagename = pathlib.Path(imagename).resolve()
    
    if type(compression_level) != int:
        compression_level = int(compression_level)

    if outname and type(outname) != pathlib.Path:
        outname = pathlib.Path(outname).with_suffix(".webp")
    elif outname and type(outname) == pathlib.Path and outname.suffix != ".webp":
        outname = outname.with_suffix(".webp")
    else:
        outname = imagename.with_suffix(".webp")
    
    outbytes: bytes = bytes()
    def useinput():
        # if verbose:
        print("Image is already in webp format, using input data.")
        with imagename.open("rb") as imagedata:
            outbytes = imagedata.read()
    try:
        Image.open(imagename).verify()
        
        with BytesIO() as out, Image.open(imagename) as inimage:
            formatlist = ["WebP", "webp", "WEBP"]
            if inimage and inimage.format in formatlist:
                inimage.close()
                useinput()
            elif inimage and inimage.format not in formatlist:
                if lossless:
                    # if verbose:
                    print("Saving image in lossless WebP format.")
                    inimage.save(out, format="WEBP", lossless=True, quality=compression_level)
                else:
                    # if verbose:
                    print("Saving image in lossy WebP format.")
                    inimage.save(out, format="WEBP", quality=compression_level)
                if out:
                    outbytes = out.getvalue()
            else:
                print("if statements went wrong somewhere, using input data.")
                inimage.close()
                useinput()
    except Exception:
        raise
    else:
        if type(outbytes) == bytes:
            return FileInfo(str(outname), outbytes)
        else:
            raise TypeError("Data in the outbytes variable is not of type: bytes")

__all__ = ["webpconvert"]