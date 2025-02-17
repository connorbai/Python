from importlib.util import find_spec

spec = find_spec("pandas")
if spec is not None:
    import pandas as pd

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 5000)
