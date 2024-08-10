import polars as pl


def _check_input_maybe_try_transform(input):
    if isinstance(input, pl.DataFrame):
        if input.height == 0 or input.width == 0:
            raise ValueError("Input data frame must have rows and columns")
        else:
            return input
    else:
        print("Attempting to convert input to polars.DataFrame")
        try:
            out = pl.DataFrame(input)
        except Exception as e:
            print(f"Error occurred during attempted conversion: {e}")
    if out.height == 0 or out.width == 0:
        raise ValueError("Input not compatible")
