import math
import random
from shapely.geometry import Point, LineString, MultiLineString

def offset_point(pt: Point, max_offset: float = 0.00005) -> Point:
  """Circular offset around a point, use small offset for lat/lon..."""
  angle = random.uniform(0, 2 * math.pi)
  radius = random.uniform(0, max_offset)
  dx = math.cos(angle) * radius
  dy = math.sin(angle) * radius
  return Point(pt.x + dx, pt.y + dy)

def line_to_point(geom, interpolation=0.0):
  if geom is None or geom.is_empty:
    return None
  if isinstance(geom, LineString):
    return geom.interpolate(interpolation, normalized=True)
  if isinstance(geom, MultiLineString):
    merged = max(geom.geoms, key=lambda g: g.length)
    return merged.interpolate(interpolation, normalized=True)
  return geom
