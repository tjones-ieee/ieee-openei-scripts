import math
import random
from shapely.geometry import Point

def offset_point(pt: Point, max_offset: float = 0.00005) -> Point:
  """Circular offset around a point, use small offset for lat/lon..."""
  angle = random.uniform(0, 2 * math.pi)
  radius = random.uniform(0, max_offset)
  dx = math.cos(angle) * radius
  dy = math.sin(angle) * radius
  return Point(pt.x + dx, pt.y + dy)
