from datetime import datetime, timedelta, timezone

def postgres_timestamptz_to_datetime(int64_value):
    """
    Convert a PostgreSQL internal timestamptz (int64) value to a UTC datetime.

    Args:
        int64_value (int): Microseconds since 2000-01-01 00:00:00 UTC (PostgreSQL epoch)

    Returns:
        datetime: UTC datetime object
    """
    # PostgreSQL epoch: 2000-01-01 00:00:00 UTC
    postgres_epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)
    
    # Convert microseconds to timedelta
    delta = timedelta(microseconds=int64_value)
    
    # Return actual UTC datetime
    return postgres_epoch + delta

def hex_le_to_int64(hex_str):
    """
    Convert a little-endian hex string to a signed int64 integer.
    
    Args:
        hex_str (str): Hex string like '0a00000000000000'
        
    Returns:
        int: Corresponding signed 64-bit integer
    """
    if len(hex_str) != 16:
        raise ValueError("Hex string must be exactly 16 characters (8 bytes).")
    
    # Convert hex string to bytes
    byte_data = bytes.fromhex(hex_str)
    
    # Convert little-endian bytes to int64
    return int.from_bytes(byte_data, byteorder='little', signed=True)
    
    
# hex_input = 'a922e78e1bdd0200'
# result = postgres_timestamptz_to_datetime(hex_le_to_int64(hex_input))
# print(f"Converted int64: {result}")    

print(postgres_timestamptz_to_datetime(662770800000000))