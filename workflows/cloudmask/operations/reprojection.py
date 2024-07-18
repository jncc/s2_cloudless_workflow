import math
from osgeo import osr

def getEPSGCodeFromProjection(wkt) -> str:
    srs = osr.SpatialReference(wkt)
    return srs.GetAuthorityCode(None)

def getReprojectedBoundingBox(xMin:int, yMin:int, xMax:int, yMax:int, inputProjection:osr.SpatialReference, outputProjection:osr.SpatialReference, densifyPoints:int=21) -> tuple[float, float, float, float]:
    transformer = osr.CoordinateTransformation(inputProjection, outputProjection)
    return transformer.TransformBounds(xMin, yMin, xMax, yMax, densifyPoints)

def getBoundBoxPinnedToGrid(xMin:float, yMin:float, xMax:float, yMax:float, xPixelResolution:int, yPixelResolution:int) -> tuple[float, float, float, float]:
    yPixelResolutionAbsolute = math.fabs(yPixelResolution)

    xMinPinned = math.floor(xMin / xPixelResolution) * xPixelResolution
    yMinPinned = math.floor(yMin / yPixelResolutionAbsolute) * yPixelResolutionAbsolute
    xMaxPinned = math.ceil(xMax / xPixelResolution) * xPixelResolution
    yMaxPinned = math.ceil(yMax / yPixelResolutionAbsolute) * yPixelResolutionAbsolute

    return (xMinPinned, yMinPinned, xMaxPinned, yMaxPinned)