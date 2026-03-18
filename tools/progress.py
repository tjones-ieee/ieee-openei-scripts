def print_progress(d, t):
  p = d / t if t > 0 else 0
  bar_length = 100
  filled_length = int(bar_length * p)
  bar = '█' * filled_length + '░' * (bar_length - filled_length)
  print(f"\rProgress: [{bar}] {100*p:.3f}%", end="", flush=True)