import socket  # noqa: F401
import selectors  # noqa: F401
import sys
import time
import random
import fnmatch
import math
import hashlib
import os

store = {}
key_versions = {}
pending_blpop_requests = []
pending_xread_requests = []
transaction_commands = {}
watched_keys = {}
connection_buffers = {}
subscribed_connections = {}
channel_subscribers = {}

role = "master"
listen_port = 6379
rdb_dir = "/app"
dbfilename = "dump.rdb"
master_host = None
master_port = None
master_connection = None
master_replid = ""
master_repl_offset = 0
replica_processed_offset = 0
replica_ack_offsets = {}
replica_connections = set()
default_user_nopass = True
default_user_password_hashes = []
authenticated_connections = set()
appendonly_enabled = False
appenddirname = "appendonlydir"
appendfilename = "appendonly.aof"
aof_dir_path = None
aof_incr_file_path = None
aof_manifest_file_path = None
aof_replay_in_progress = False

EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

def parse_resp_command(data):
  parts = data.split(b"\r\n")
  items = []
  
  i = 0
  while i < len(parts):
    part = parts[i]
    if not part:
      i += 1
      continue
    
    if part.startswith(b"*"):
      i += 1
      continue
    
    if part.startswith(b"$"):
      if i + 1 < len(parts):
        items.append(parts[i + 1].decode())
      i += 2
      continue
    
    i += 1
    
  return items


def parse_resp_array_message(buffer, start_index):
  if start_index >= len(buffer) or buffer[start_index:start_index + 1] != b"*":
    return None

  line_end = buffer.find(b"\r\n", start_index)
  if line_end == -1:
    return None

  try:
    item_count = int(buffer[start_index + 1:line_end])
  except ValueError:
    return None

  index = line_end + 2
  parts = []

  for _ in range(item_count):
    if index >= len(buffer):
      return None

    if buffer[index:index + 1] != b"$":
      return None

    bulk_end = buffer.find(b"\r\n", index)
    if bulk_end == -1:
      return None

    try:
      bulk_len = int(buffer[index + 1:bulk_end])
    except ValueError:
      return None

    value_start = bulk_end + 2
    value_end = value_start + bulk_len
    if value_end + 2 > len(buffer):
      return None

    if buffer[value_end:value_end + 2] != b"\r\n":
      return None

    parts.append(buffer[value_start:value_end].decode())
    index = value_end + 2

  return parts, index


def extract_resp_commands(connection, data):
  existing = connection_buffers.get(connection, b"")
  buffer = existing + data

  commands = []
  offset = 0

  while offset < len(buffer):
    while offset + 1 < len(buffer) and buffer[offset:offset + 2] == b"\r\n":
      offset += 2

    parsed = parse_resp_array_message(buffer, offset)
    if parsed is None:
      break

    parts, next_offset = parsed
    raw_command = buffer[offset:next_offset]
    commands.append((parts, raw_command))
    offset = next_offset

  connection_buffers[connection] = buffer[offset:]
  return commands


def parse_server_config(argv):
  global role
  global listen_port
  global rdb_dir
  global dbfilename
  global master_host
  global master_port
  global appendonly_enabled
  global appenddirname
  global appendfilename

  index = 1
  while index < len(argv):
    token = argv[index]

    if token == "--port" and index + 1 < len(argv):
      listen_port = int(argv[index + 1])
      index += 2
      continue

    if token == "--dir" and index + 1 < len(argv):
      rdb_dir = argv[index + 1]
      index += 2
      continue

    if token == "--dbfilename" and index + 1 < len(argv):
      dbfilename = argv[index + 1]
      index += 2
      continue

    if token == "--replicaof" and index + 1 < len(argv):
      replicaof_value = argv[index + 1]
      host_port = replicaof_value.split()
      if len(host_port) == 2:
        master_host = host_port[0]
        master_port = int(host_port[1])
        index += 2
      elif index + 2 < len(argv):
        master_host = argv[index + 1]
        master_port = int(argv[index + 2])
        index += 3
      else:
        index += 1
        continue

      role = "slave"
      continue

    if token == "--appendonly" and index + 1 < len(argv):
      appendonly_enabled = argv[index + 1].lower() == "yes"
      index += 2
      continue

    if token == "--appenddirname" and index + 1 < len(argv):
      appenddirname = argv[index + 1]
      index += 2
      continue

    if token == "--appendfilename" and index + 1 < len(argv):
      appendfilename = argv[index + 1]
      index += 2
      continue

    index += 1


def random_replid():
  alphabet = "0123456789abcdef"
  return "".join(random.choice(alphabet) for _ in range(40))


def get_rdb_path():
  separator = "\\" if "\\" in rdb_dir else "/"
  return f"{rdb_dir}{separator}{dbfilename}"


def get_aof_dir_path():
  return os.path.join(rdb_dir, appenddirname)


def get_aof_incr_file_name():
  return f"{appendfilename}.1.incr.aof"


def get_aof_manifest_file_name():
  return f"{appendfilename}.manifest"


def is_aof_write_command(command):
  return command in {"SET", "INCR", "RPUSH", "LPUSH", "LPOP", "XADD", "ZADD", "ZREM", "GEOADD"}


def should_persist_command(command):
  if not appendonly_enabled:
    return False

  if aof_replay_in_progress:
    return False

  return is_aof_write_command(command)


def initialize_aof_storage():
  global aof_dir_path
  global aof_incr_file_path
  global aof_manifest_file_path

  if not appendonly_enabled:
    return

  aof_dir_path = get_aof_dir_path()
  os.makedirs(aof_dir_path, exist_ok=True)

  aof_incr_file_path = os.path.join(aof_dir_path, get_aof_incr_file_name())
  aof_manifest_file_path = os.path.join(aof_dir_path, get_aof_manifest_file_name())

  with open(aof_incr_file_path, "ab"):
    pass

  manifest_contents = f"file {get_aof_incr_file_name()} seq 1 type i\n"
  with open(aof_manifest_file_path, "w", encoding="utf-8") as manifest_file:
    manifest_file.write(manifest_contents)


def append_command_to_aof(command, raw_command):
  if not should_persist_command(command):
    return

  if raw_command is None:
    return

  if aof_incr_file_path is None:
    initialize_aof_storage()

  with open(aof_incr_file_path, "ab") as aof_file:
    aof_file.write(raw_command)


def parse_resp_commands_from_bytes(buffer):
  commands = []
  offset = 0

  while offset < len(buffer):
    while offset + 1 < len(buffer) and buffer[offset:offset + 2] == b"\r\n":
      offset += 2

    parsed = parse_resp_array_message(buffer, offset)
    if parsed is None:
      break

    parts, next_offset = parsed
    commands.append(parts)
    offset = next_offset

  return commands


def replay_aof_if_enabled():
  global aof_replay_in_progress

  if not appendonly_enabled:
    return

  if aof_incr_file_path is None:
    return

  try:
    with open(aof_incr_file_path, "rb") as aof_file:
      data = aof_file.read()
  except FileNotFoundError:
    return

  if not data:
    return

  commands = parse_resp_commands_from_bytes(data)
  aof_replay_in_progress = True
  try:
    for command_parts in commands:
      if not command_parts:
        continue

      command = command_parts[0].upper()
      if is_aof_write_command(command):
        apply_replicated_write(command_parts)
  finally:
    aof_replay_in_progress = False


def read_rdb_length(data, index):
  first = data[index]
  index += 1
  mode = (first & 0b11000000) >> 6

  if mode == 0:
    return first & 0b00111111, index, False

  if mode == 1:
    second = data[index]
    index += 1
    value = ((first & 0b00111111) << 8) | second
    return value, index, False

  if mode == 2:
    value = int.from_bytes(data[index:index + 4], "big")
    index += 4
    return value, index, False

  return first & 0b00111111, index, True


def read_rdb_string(data, index):
  value, index, is_encoded = read_rdb_length(data, index)

  if is_encoded:
    if value == 0:
      raw_value = int.from_bytes(data[index:index + 1], "little", signed=True)
      index += 1
      return str(raw_value), index

    if value == 1:
      raw_value = int.from_bytes(data[index:index + 2], "little", signed=True)
      index += 2
      return str(raw_value), index

    if value == 2:
      raw_value = int.from_bytes(data[index:index + 4], "little", signed=True)
      index += 4
      return str(raw_value), index

    raise ValueError("Unsupported encoded string in RDB")

  text = data[index:index + value].decode("utf-8", errors="strict")
  index += value
  return text, index


def unix_ms_to_monotonic_deadline(expires_unix_ms):
  now_unix_ms = int(time.time() * 1000)
  remaining_ms = expires_unix_ms - now_unix_ms
  if remaining_ms <= 0:
    return None

  return time.monotonic() + (remaining_ms / 1000)


def load_rdb_file():
  path = get_rdb_path()

  try:
    with open(path, "rb") as file_handle:
      data = file_handle.read()
  except FileNotFoundError:
    return

  if not data.startswith(b"REDIS"):
    return

  index = 9
  expires_unix_ms = None

  while index < len(data):
    opcode = data[index]
    index += 1

    if opcode == 0xFF:
      break

    if opcode == 0xFA:
      _, index = read_rdb_string(data, index)
      _, index = read_rdb_string(data, index)
      continue

    if opcode == 0xFE:
      _, index, _ = read_rdb_length(data, index)
      continue

    if opcode == 0xFB:
      _, index, _ = read_rdb_length(data, index)
      _, index, _ = read_rdb_length(data, index)
      continue

    if opcode == 0xFC:
      expires_unix_ms = int.from_bytes(data[index:index + 8], "little")
      index += 8
      continue

    if opcode == 0xFD:
      expires_unix_seconds = int.from_bytes(data[index:index + 4], "little")
      expires_unix_ms = expires_unix_seconds * 1000
      index += 4
      continue

    if opcode == 0x00:
      key, index = read_rdb_string(data, index)
      value, index = read_rdb_string(data, index)

      expires_at = None
      if expires_unix_ms is not None:
        expires_at = unix_ms_to_monotonic_deadline(expires_unix_ms)

      if expires_unix_ms is None or expires_at is not None:
        store[key] = make_string_entry(value, expires_at)

      expires_unix_ms = None
      continue

    raise ValueError("Unsupported RDB opcode")


def connect_to_master_and_handshake(selector):
  global master_connection

  if master_host is None or master_port is None:
    return

  connection = socket.create_connection((master_host, master_port))
  connection.settimeout(5)

  connection.sendall(encode_array(["PING"]))
  read_simple_string_response(connection)

  connection.sendall(encode_array(["REPLCONF", "listening-port", str(listen_port)]))
  read_simple_string_response(connection)

  connection.sendall(encode_array(["REPLCONF", "capa", "psync2"]))
  read_simple_string_response(connection)

  connection.sendall(encode_array(["PSYNC", "?", "-1"]))
  read_simple_string_response(connection)
  read_bulk_string_response(connection)

  connection.setblocking(False)
  connection.settimeout(None)
  master_connection = connection
  selector.register(connection, selectors.EVENT_READ, read_master)

  buffered_master_commands = extract_resp_commands(connection, b"")
  if buffered_master_commands:
    process_master_commands(connection, selector, buffered_master_commands)


def read_line_blocking(connection):
  data = connection_buffers.get(connection, b"")
  while b"\r\n" not in data:
    chunk = connection.recv(1024)
    if not chunk:
      raise ConnectionError("Connection closed")
    data += chunk

  line, remainder = data.split(b"\r\n", 1)
  connection_buffers[connection] = remainder
  return line


def read_simple_string_response(connection):
  buffered = connection_buffers.get(connection, b"")
  if b"\r\n" not in buffered:
    line = read_line_blocking(connection)
  else:
    line, remainder = buffered.split(b"\r\n", 1)
    connection_buffers[connection] = remainder

  if line[:1] not in (b"+", b"-"):
    raise ValueError("Unexpected response during handshake")


def read_exact_blocking(connection, size):
  buffered = connection_buffers.get(connection, b"")
  data = buffered
  while len(data) < size:
    chunk = connection.recv(4096)
    if not chunk:
      raise ConnectionError("Connection closed")
    data += chunk

  connection_buffers[connection] = data[size:]
  return data[:size]


def read_bulk_string_response(connection):
  line = read_line_blocking(connection)
  if line[:1] != b"$":
    raise ValueError("Expected bulk response")

  bulk_len = int(line[1:])
  read_exact_blocking(connection, bulk_len)

  buffered = connection_buffers.get(connection, b"")
  if buffered.startswith(b"\r\n"):
    connection_buffers[connection] = buffered[2:]


def process_master_commands(connection, selector, commands):
  global replica_processed_offset

  for command_parts, raw_command in commands:
    if not command_parts:
      continue

    command = command_parts[0].upper()
    if command == "REPLCONF" and len(command_parts) >= 3 and command_parts[1].upper() == "GETACK":
      ack_payload = encode_array(["REPLCONF", "ACK", str(replica_processed_offset)])
      try:
        connection.sendall(ack_payload)
      except OSError:
        pass
      replica_processed_offset += len(raw_command)
      continue

    was_handled = execute_command(connection, selector, command_parts, raw_command=raw_command, send_response=False, from_master=True)
    if not was_handled:
      apply_replicated_write(command_parts)

    replica_processed_offset += len(raw_command)


def replication_info_text():
  lines = [f"role:{role}"]
  if role == "master":
    lines.append(f"master_replid:{master_replid}")
    lines.append(f"master_repl_offset:{master_repl_offset}")
  else:
    lines.append(f"master_host:{master_host}")
    lines.append(f"master_port:{master_port}")
    lines.append("master_link_status:up")
  return "\r\n".join(lines) + "\r\n"


def replicate_and_persist(raw_command, command):
  if role == "master" and raw_command is not None:
    propagate_to_replicas(raw_command)

  append_command_to_aof(command, raw_command)


def hash_password(password):
  return hashlib.sha256(password.encode()).hexdigest()


def is_authentication_required():
  return (not default_user_nopass) and len(default_user_password_hashes) > 0


def is_connection_authenticated(connection):
  if not is_authentication_required():
    return True

  return connection in authenticated_connections


def apply_default_user_password(password):
  global default_user_nopass

  default_user_nopass = False
  hashed = hash_password(password)
  if hashed not in default_user_password_hashes:
    default_user_password_hashes.append(hashed)


def clear_default_user_passwords():
  default_user_password_hashes.clear()


def default_user_flags():
  flags = ["on"]
  if default_user_nopass:
    flags.append("nopass")
  flags.extend(["allkeys", "allchannels", "allcommands"])
  return flags


def default_user_acl_response():
  return [
    "flags", default_user_flags(),
    "passwords", list(default_user_password_hashes),
    "commands", "+@all",
    "keys", "~*",
    "channels", "&*",
    "selectors", [],
  ]


def apply_acl_setuser_default(connection, modifiers):
  global default_user_nopass

  changed_auth_config = False

  for modifier in modifiers:
    lower = modifier.lower()

    if lower == "on":
      continue

    if lower == "nopass":
      default_user_nopass = True
      clear_default_user_passwords()
      changed_auth_config = True
      continue

    if lower == "resetpass":
      clear_default_user_passwords()
      default_user_nopass = False
      changed_auth_config = True
      continue

    if lower in ("allkeys", "allchannels", "allcommands"):
      continue

    if modifier.startswith(">"):
      apply_default_user_password(modifier[1:])
      changed_auth_config = True
      continue

    return make_error_value("ERR Error in ACL SETUSER modifier")

  if changed_auth_config:
    authenticated_connections.clear()
    authenticated_connections.add(connection)

  return make_simple_value("OK")


def is_write_command(command):
  return command in {"SET", "INCR", "RPUSH", "LPUSH", "LPOP", "XADD"}


def propagate_to_replicas(raw_command):
  global master_repl_offset

  if not replica_connections:
    return

  master_repl_offset += len(raw_command)

  dead_connections = []
  for replica_connection in replica_connections:
    try:
      replica_connection.sendall(raw_command)
    except OSError:
      dead_connections.append(replica_connection)

  for dead_connection in dead_connections:
    replica_connections.discard(dead_connection)
    replica_ack_offsets.pop(dead_connection, None)


def request_replica_acks():
  request = encode_array(["REPLCONF", "GETACK", "*"])
  dead_connections = []
  for replica_connection in replica_connections:
    try:
      replica_connection.sendall(request)
    except OSError:
      dead_connections.append(replica_connection)

  for dead_connection in dead_connections:
    replica_connections.discard(dead_connection)
    replica_ack_offsets.pop(dead_connection, None)


def count_acked_replicas():
  if master_repl_offset == 0:
    return len(replica_connections)

  count = 0
  for replica_connection in replica_connections:
    if replica_ack_offsets.get(replica_connection, -1) >= master_repl_offset:
      count += 1

  return count


def handle_wait(connection, selector, command_parts):
  if len(command_parts) < 3:
    connection.sendall(encode_error("ERR wrong number of arguments for 'WAIT' command"))
    return

  replicas_needed = int(command_parts[1])
  timeout_ms = int(command_parts[2])

  if not replica_connections:
    connection.sendall(encode_integer(0))
    return

  if count_acked_replicas() >= replicas_needed:
    connection.sendall(encode_integer(count_acked_replicas()))
    return

  request_replica_acks()
  deadline = time.monotonic() + (timeout_ms / 1000)

  while time.monotonic() < deadline:
    if count_acked_replicas() >= replicas_needed:
      break

    events = selector.select(timeout=0.01)
    for key, _ in events:
      callback = key.data
      callback(key.fileobj, selector)

    expire_pending_blpop_requests()
    expire_pending_xread_requests()

  connection.sendall(encode_integer(count_acked_replicas()))

def encode_simple_string(value):
  return f"+{value}\r\n".encode()

def encode_bulk_string(value):
  if value is None:
    return b"$-1\r\n"
  return f"${len(value)}\r\n{value}\r\n".encode()


def encode_integer(value):
  return f":{value}\r\n".encode()


def encode_error(message):
  return f"-{message}\r\n".encode()


def encode_array(values):
  if values is None:
    return b"*-1\r\n"

  return encode_resp(values)


def encode_resp(value):
  if value is None:
    return b"$-1\r\n"

  if isinstance(value, dict) and value.get("type") == "null_array":
    return b"*-1\r\n"

  if isinstance(value, dict) and value.get("type") == "error":
    return encode_error(value["message"])

  if isinstance(value, dict) and value.get("type") == "simple":
    return encode_simple_string(value["value"])

  if isinstance(value, int):
    return encode_integer(value)

  if isinstance(value, str):
    return encode_bulk_string(value)

  if isinstance(value, list):
    response = f"*{len(value)}\r\n".encode()
    for item in value:
      response += encode_resp(item)
    return response

  raise TypeError(f"Unsupported RESP value: {type(value)!r}")


def make_string_entry(value, expires_at=None):
  return {"type": "string", "value": value, "expires_at": expires_at}


def make_list_entry(values=None):
  if values is None:
    values = []

  return {"type": "list", "value": values, "expires_at": None}


def make_stream_entry(values=None, last_ms=0, last_seq=0):
  if values is None:
    values = []
  
  return {
    "type": "stream",
    "value": values,
    "expires_at": None,
    "last_ms": last_ms,
    "last_seq": last_seq,
  }


def make_zset_entry(values=None, geo=None):
  if values is None:
    values = {}
  if geo is None:
    geo = {}

  return {
    "type": "zset",
    "value": values,
    "geo": geo,
    "expires_at": None,
  }


def get_entry(key):
  entry = store.get(key)
  if entry is None:
    return None

  expires_at = entry["expires_at"]
  if expires_at is not None and time.monotonic() >= expires_at:
    touch_key(key)
    del store[key]
    return None

  return entry


def touch_key(key):
  key_versions[key] = key_versions.get(key, 0) + 1


def get_key_version(key):
  get_entry(key)
  return key_versions.get(key, 0)


def get_value(key):
  entry = get_entry(key)
  if entry is None:
    return None

  if entry["type"] != "string":
    return None

  return entry["value"]


def make_error_value(message):
  return {"type": "error", "message": message}


def make_simple_value(value):
  return {"type": "simple", "value": value}


def make_null_array_value():
  return {"type": "null_array"}


def watch_keys_for_connection(connection, keys):
  watched = watched_keys.setdefault(connection, {})
  for key in keys:
    watched[key] = get_key_version(key)


def clear_watched_keys(connection):
  watched_keys.pop(connection, None)


def transaction_is_dirty(connection):
  watched = watched_keys.get(connection)
  if not watched:
    return False

  for key, version in watched.items():
    if get_key_version(key) != version:
      return True

  return False


def get_list_for_write(key):
  entry = get_entry(key)
  if entry is None:
    store[key] = make_list_entry()
    touch_key(key)
    return store[key]["value"]

  if entry["type"] != "list":
    return None

  return entry["value"]

def get_stream_for_write(key):
  entry = get_entry(key)
  if entry is None:
    store[key] = make_stream_entry()
    touch_key(key)
    return store[key]["value"]
  
  if entry["type"] != "stream":
    return None
  
  return entry["value"]


def get_stream_for_read(key):
  entry = get_entry(key)
  if entry is None:
    return []

  if entry["type"] != "stream":
    return None

  return entry["value"]


def get_zset_entry_for_write(key):
  entry = get_entry(key)
  if entry is None:
    store[key] = make_zset_entry()
    touch_key(key)
    return store[key]

  if entry["type"] != "zset":
    return None

  return entry


def get_zset_entry_for_read(key):
  entry = get_entry(key)
  if entry is None:
    return None

  if entry["type"] != "zset":
    return "WRONGTYPE"

  return entry


def zset_sorted_items(zset_values):
  return sorted(zset_values.items(), key=lambda item: (item[1], item[0]))


def format_float(value):
  if value == int(value):
    return str(int(value))
  return f"{value:.17g}"


GEO_LAT_MIN = -85.05112878
GEO_LAT_MAX = 85.05112878
GEO_LON_MIN = -180.0
GEO_LON_MAX = 180.0
GEO_EARTH_RADIUS_METERS = 6372797.560856


def validate_geo_coordinates(longitude, latitude):
  if longitude < GEO_LON_MIN or longitude > GEO_LON_MAX:
    return False
  if latitude < GEO_LAT_MIN or latitude > GEO_LAT_MAX:
    return False
  return True


def interleave_26_bits(x_value, y_value):
  result = 0
  for bit in range(26):
    result |= ((x_value >> bit) & 1) << (2 * bit + 1)
    result |= ((y_value >> bit) & 1) << (2 * bit)
  return result


def deinterleave_26_bits(value):
  x_value = 0
  y_value = 0
  for bit in range(26):
    y_value |= ((value >> (2 * bit)) & 1) << bit
    x_value |= ((value >> (2 * bit + 1)) & 1) << bit
  return x_value, y_value


def calculate_geo_score(longitude, latitude):
  lon_normalized = (longitude - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN)
  lat_normalized = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN)

  max_value = 1 << 26
  lon_fixed = int(lon_normalized * max_value)
  lat_fixed = int(lat_normalized * max_value)

  lon_fixed = max(0, min(lon_fixed, max_value - 1))
  lat_fixed = max(0, min(lat_fixed, max_value - 1))

  return float(interleave_26_bits(lon_fixed, lat_fixed))


def decode_geo_score(score):
  score_int = int(score)
  lon_fixed, lat_fixed = deinterleave_26_bits(score_int)

  max_value = 1 << 26
  longitude = ((lon_fixed + 0.5) / max_value) * (GEO_LON_MAX - GEO_LON_MIN) + GEO_LON_MIN
  latitude = ((lat_fixed + 0.5) / max_value) * (GEO_LAT_MAX - GEO_LAT_MIN) + GEO_LAT_MIN
  return longitude, latitude


def get_geo_coordinates(entry, member):
  score = entry["value"].get(member)
  if score is not None:
    return decode_geo_score(score)

  return entry["geo"].get(member)


def geo_distance_meters(lon1, lat1, lon2, lat2):
  lat1_rad = math.radians(lat1)
  lat2_rad = math.radians(lat2)
  delta_lat = math.radians(lat2 - lat1)
  delta_lon = math.radians(lon2 - lon1)

  a_value = (
    math.sin(delta_lat / 2) ** 2
    + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
  )
  c_value = 2 * math.atan2(math.sqrt(a_value), math.sqrt(1 - a_value))
  return GEO_EARTH_RADIUS_METERS * c_value


def unit_to_meters_multiplier(unit):
  unit_lower = unit.lower()
  if unit_lower == "m":
    return 1.0
  if unit_lower == "km":
    return 1000.0
  if unit_lower == "mi":
    return 1609.34
  if unit_lower == "ft":
    return 0.3048
  return None


def parse_geosearch_arguments(command_parts):
  if len(command_parts) < 8:
    return None

  key = command_parts[1]
  index = 2
  center_lon = None
  center_lat = None
  from_member = None

  origin_mode = command_parts[index].upper()
  try:
    if origin_mode == "FROMMEMBER":
      if index + 1 >= len(command_parts):
        return None
      from_member = command_parts[index + 1]
      index += 2
    elif origin_mode == "FROMLONLAT":
      if index + 2 >= len(command_parts):
        return None
      center_lon = float(command_parts[index + 1])
      center_lat = float(command_parts[index + 2])
      index += 3
    else:
      return None
  except ValueError:
    return None

  if index >= len(command_parts) or command_parts[index].upper() != "BYRADIUS":
    return None

  if index + 2 >= len(command_parts):
    return None

  try:
    radius = float(command_parts[index + 1])
  except ValueError:
    return None
  unit = command_parts[index + 2]
  index += 3

  withdist = False
  withcoord = False
  sort_desc = False
  count = None

  while index < len(command_parts):
    option = command_parts[index].upper()
    if option == "WITHDIST":
      withdist = True
      index += 1
      continue
    if option == "WITHCOORD":
      withcoord = True
      index += 1
      continue
    if option == "ASC":
      sort_desc = False
      index += 1
      continue
    if option == "DESC":
      sort_desc = True
      index += 1
      continue
    if option == "COUNT" and index + 1 < len(command_parts):
      try:
        count = int(command_parts[index + 1])
      except ValueError:
        return None
      index += 2
      continue
    return None

  return {
    "key": key,
    "center_lon": center_lon,
    "center_lat": center_lat,
    "from_member": from_member,
    "radius": radius,
    "unit": unit,
    "withdist": withdist,
    "withcoord": withcoord,
    "sort_desc": sort_desc,
    "count": count,
  }


def run_geosearch(query, entry):
  geo_values = entry["geo"]
  center_lon = query["center_lon"]
  center_lat = query["center_lat"]

  if query["from_member"] is not None:
    base = get_geo_coordinates(entry, query["from_member"])
    if base is None:
      return []
    center_lon, center_lat = base

  multiplier = unit_to_meters_multiplier(query["unit"])
  if multiplier is None:
    return "ERR"

  radius_meters = query["radius"] * multiplier
  distances = []
  for member, (lon, lat) in geo_values.items():
    distance_meters = geo_distance_meters(center_lon, center_lat, lon, lat)
    if distance_meters <= radius_meters:
      distances.append((member, distance_meters, lon, lat))

  distances.sort(key=lambda item: item[1], reverse=query["sort_desc"])

  if query["count"] is not None:
    distances = distances[:query["count"]]

  results = []
  for member, distance_meters, lon, lat in distances:
    if not query["withdist"] and not query["withcoord"]:
      results.append(member)
      continue

    item = [member]
    if query["withdist"]:
      item.append(format_float(distance_meters / multiplier))
    if query["withcoord"]:
      item.append([format_float(lon), format_float(lat)])
    results.append(item)

  return results

def generate_stream_id(entry, requested_ms=None):
  current_ms = int(time.time() * 1000) if requested_ms is None else requested_ms
  current_ms = max(current_ms, entry["last_ms"])

  if current_ms == entry["last_ms"]:
    entry["last_seq"] += 1
  else:
    entry["last_ms"] = current_ms
    entry["last_seq"] = 0

  return f"{entry['last_ms']}-{entry['last_seq']}"


def get_stream_last_id(stream_values):
  if not stream_values:
    return None

  return parse_stream_id(stream_values[-1]["id"])


def build_stream_entries(stream_values, cursor_id):
  entries = []

  for stream_item in stream_values:
    item_id = parse_stream_id(stream_item["id"])
    if item_id is None:
      continue

    if cursor_id is None or item_id > cursor_id:
      fields = []
      for field_name, field_value in stream_item["fields"].items():
        fields.append(field_name)
        fields.append(field_value)

      entries.append([stream_item["id"], fields])

  return entries


def build_xread_response(keys, cursors):
  response = []

  for key, cursor_id in zip(keys, cursors):
    entry = get_entry(key)
    if entry is None:
      continue

    if entry["type"] != "stream":
      return "WRONGTYPE"

    stream_entries = build_stream_entries(entry["value"], cursor_id)
    if stream_entries:
      response.append([key, stream_entries])

  if not response:
    return None

  return response


def resolve_xread_cursor(key, token):
  if token != "$":
    parsed_id = parse_stream_id(token)
    if parsed_id is None or parsed_id in ("-", "+"):
      return None

    return parsed_id

  entry = get_entry(key)
  if entry is None:
    return None

  if entry["type"] != "stream":
    return "WRONGTYPE"

  return get_stream_last_id(entry["value"])


def get_pending_request_deadlines():
  deadlines = []

  for request in pending_blpop_requests:
    deadline = request["deadline"]
    if deadline is not None:
      deadlines.append(deadline)

  for request in pending_xread_requests:
    deadline = request["deadline"]
    if deadline is not None:
      deadlines.append(deadline)

  return deadlines


def get_transaction_queue(connection):
  return transaction_commands.setdefault(connection, [])


def clear_transaction(connection):
  transaction_commands.pop(connection, None)


def is_transaction_active(connection):
  return connection in transaction_commands


def apply_incr(key):
  entry = get_entry(key)
  if entry is None:
    store[key] = make_string_entry("1")
    touch_key(key)
    return 1

  if entry["type"] != "string":
    return make_error_value("ERR value is not an integer or out of range")

  try:
    current_value = int(entry["value"])
  except ValueError:
    return make_error_value("ERR value is not an integer or out of range")

  current_value += 1
  entry["value"] = str(current_value)
  touch_key(key)
  return current_value


def execute_transaction_command(command_parts):
  command = command_parts[0].upper()

  if command == "INCR" and len(command_parts) >= 2:
    return apply_incr(command_parts[1])

  if command == "GET" and len(command_parts) >= 2:
    return get_value(command_parts[1])

  if command == "SET" and len(command_parts) >= 3:
    key = command_parts[1]
    value = command_parts[2]

    expires_at = None
    if len(command_parts) >= 5:
      option = command_parts[3].upper()
      option_value = command_parts[4]

      if option == "PX":
        expires_at = time.monotonic() + (int(option_value) / 1000)
      elif option == "EX":
        expires_at = time.monotonic() + int(option_value)

    store[key] = make_string_entry(value, expires_at)
    touch_key(key)
    return make_simple_value("OK")

  if command == "TYPE" and len(command_parts) >= 2:
    entry = get_entry(command_parts[1])
    if entry is None:
      return make_simple_value("none")

    return make_simple_value(entry["type"])

  if command == "PING":
    return make_simple_value("PONG")

  if command == "ECHO" and len(command_parts) >= 2:
    return command_parts[1]

  return make_error_value("ERR unknown command")


def remove_pending_requests_for_connection(connection):
  index = 0
  while index < len(pending_blpop_requests):
    request = pending_blpop_requests[index]
    if request["connection"] == connection:
      pending_blpop_requests.pop(index)
      continue

    index += 1


  clear_transaction(connection)
  clear_watched_keys(connection)
  unsubscribe_all_channels(connection)
  connection_buffers.pop(connection, None)
  replica_connections.discard(connection)
  replica_ack_offsets.pop(connection, None)
  authenticated_connections.discard(connection)

  index = 0
  while index < len(pending_xread_requests):
    request = pending_xread_requests[index]
    if request["connection"] == connection:
      pending_xread_requests.pop(index)
      continue

    index += 1

def get_list_for_read(key):
  entry = get_entry(key)
  if entry is None:
    return []

  if entry["type"] != "list":
    return None

  return entry["value"]


def trim_lrange(values, start, stop):
  length = len(values)
  if length == 0:
    return []

  if start < 0:
    start += length
  if stop < 0:
    stop += length

  start = max(start, 0)
  stop = min(stop, length - 1)

  if start > stop or start >= length:
    return []

  return values[start:stop + 1]


def pop_from_list(key, count=None):
  list_values = get_list_for_read(key)
  if list_values is None or len(list_values) == 0:
    return None

  if count is None:
    popped = list_values.pop(0)
    touch_key(key)
    if len(list_values) == 0:
      del store[key]
    return popped

  popped_items = []
  pop_count = min(count, len(list_values))
  for _ in range(pop_count):
    popped_items.append(list_values.pop(0))

  touch_key(key)

  if len(list_values) == 0:
    del store[key]

  return popped_items


def get_subscription_count(connection):
  return len(subscribed_connections.get(connection, set()))


def subscribe_connection_to_channel(connection, channel):
  subscribed = subscribed_connections.setdefault(connection, set())
  subscribed.add(channel)

  subscribers = channel_subscribers.setdefault(channel, set())
  subscribers.add(connection)

  return len(subscribed)


def unsubscribe_connection_from_channel(connection, channel):
  subscribed = subscribed_connections.get(connection)
  if not subscribed or channel not in subscribed:
    return len(subscribed) if subscribed else 0

  subscribed.remove(channel)
  if not subscribed:
    subscribed_connections.pop(connection, None)

  subscribers = channel_subscribers.get(channel)
  if subscribers is not None:
    subscribers.discard(connection)
    if not subscribers:
      channel_subscribers.pop(channel, None)

  return len(subscribed_connections.get(connection, set()))


def unsubscribe_all_channels(connection):
  channels = list(subscribed_connections.get(connection, set()))
  for channel in channels:
    unsubscribe_connection_from_channel(connection, channel)

  return channels


def publish_message(channel, message):
  subscribers = list(channel_subscribers.get(channel, set()))

  delivered = 0
  for subscriber in subscribers:
    try:
      subscriber.sendall(encode_array(["message", channel, message]))
      delivered += 1
    except OSError:
      unsubscribe_connection_from_channel(subscriber, channel)

  return delivered


def try_pop_from_keys(keys):
  for key in keys:
    popped = pop_from_list(key)
    if popped is not None:
      return [key, popped]

  return None


def wake_pending_blpop_requests():
  index = 0
  while index < len(pending_blpop_requests):
    request = pending_blpop_requests[index]
    response = try_pop_from_keys(request["keys"])

    if response is None:
      index += 1
      continue

    connection = request["connection"]
    try:
      connection.sendall(encode_array(response))
    except OSError:
      pass

    pending_blpop_requests.pop(index)


def wake_pending_xread_requests():
  index = 0
  while index < len(pending_xread_requests):
    request = pending_xread_requests[index]
    response = build_xread_response(request["keys"], request["cursors"])

    if response is None:
      index += 1
      continue

    if response == "WRONGTYPE":
      try:
        request["connection"].sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      except OSError:
        pass
      pending_xread_requests.pop(index)
      continue

    try:
      request["connection"].sendall(encode_array(response))
    except OSError:
      pass

    pending_xread_requests.pop(index)


def expire_pending_blpop_requests():
  now = time.monotonic()

  index = 0
  while index < len(pending_blpop_requests):
    request = pending_blpop_requests[index]
    deadline = request["deadline"]

    if deadline is None or now < deadline:
      index += 1
      continue

    connection = request["connection"]
    try:
      connection.sendall(encode_array(None))
    except OSError:
      pass

    pending_blpop_requests.pop(index)


def expire_pending_xread_requests():
  now = time.monotonic()

  index = 0
  while index < len(pending_xread_requests):
    request = pending_xread_requests[index]
    deadline = request["deadline"]

    if deadline is None or now < deadline:
      index += 1
      continue

    try:
      request["connection"].sendall(encode_array(None))
    except OSError:
      pass

    pending_xread_requests.pop(index)


def get_selector_timeout():
  deadlines = get_pending_request_deadlines()
  if not deadlines:
    return None

  nearest_deadline = min(deadlines)
  return max(0, nearest_deadline - time.monotonic())

def accept_connection(server_socket, selector):
  connection, _ = server_socket.accept()
  connection.setblocking(False)
  selector.register(connection, selectors.EVENT_READ, read_client)
  
def parse_stream_id(stream_id):
  if stream_id in ("-", "+"):
    return stream_id

  parts = stream_id.split("-", 1)
  if len(parts) != 2:
    return None

  ms_text, seq_text = parts
  if not ms_text.isdigit():
    return None

  if seq_text == "*":
    return int(ms_text), "*"

  if not seq_text.isdigit():
    return None

  return int(ms_text), int(seq_text)


def parse_xread_command(command_parts):
  index = 1
  block_timeout = None

  if index < len(command_parts) and command_parts[index].upper() == "BLOCK":
    if index + 1 >= len(command_parts):
      return None

    block_timeout = int(command_parts[index + 1])
    index += 2

  if index >= len(command_parts) or command_parts[index].upper() != "STREAMS":
    return None

  values = command_parts[index + 1:]
  if len(values) < 2 or len(values) % 2 != 0:
    return None

  half = len(values) // 2
  keys = values[:half]
  id_tokens = values[half:]

  cursors = []
  for key, token in zip(keys, id_tokens):
    cursor = resolve_xread_cursor(key, token)
    if cursor == "WRONGTYPE":
      return "WRONGTYPE"

    if cursor is None and token != "$":
      return None

    cursors.append(cursor)

  return {
    "block_timeout": block_timeout,
    "keys": keys,
    "cursors": cursors,
  }


def execute_transaction_queue(connection, selector):
  if transaction_is_dirty(connection):
    clear_transaction(connection)
    clear_watched_keys(connection)
    connection.sendall(encode_array(None))
    return

  queued_commands = transaction_commands.get(connection, [])
  results = []

  for queued_command in queued_commands:
    results.append(execute_transaction_command(queued_command))

  clear_transaction(connection)
  clear_watched_keys(connection)
  connection.sendall(encode_array(results))


def apply_replicated_write(command_parts):
  command = command_parts[0].upper()

  if command == "SET" and len(command_parts) >= 3:
    key = command_parts[1]
    value = command_parts[2]

    expires_at = None
    if len(command_parts) >= 5:
      option = command_parts[3].upper()
      option_value = command_parts[4]

      if option == "PX":
        expires_at = time.monotonic() + (int(option_value) / 1000)
      elif option == "EX":
        expires_at = time.monotonic() + int(option_value)

    store[key] = make_string_entry(value, expires_at)
    touch_key(key)
    return

  if command == "INCR" and len(command_parts) >= 2:
    apply_incr(command_parts[1])
    return

  if command == "RPUSH" and len(command_parts) >= 3:
    key = command_parts[1]
    values = command_parts[2:]
    list_values = get_list_for_write(key)
    if list_values is not None:
      list_values.extend(values)
      touch_key(key)
    return

  if command == "LPUSH" and len(command_parts) >= 3:
    key = command_parts[1]
    values = command_parts[2:]
    list_values = get_list_for_write(key)
    if list_values is not None:
      for value in values:
        list_values.insert(0, value)
      touch_key(key)
    return

  if command == "LPOP" and len(command_parts) >= 2:
    key = command_parts[1]
    if len(command_parts) >= 3:
      pop_count = int(command_parts[2])
      pop_from_list(key, pop_count)
      return

    pop_from_list(key)
    return

  if command == "XADD" and len(command_parts) >= 5:
    key = command_parts[1]
    id_token = command_parts[2]
    field_values = command_parts[3:]
    if len(field_values) % 2 != 0:
      return

    stream_values = get_stream_for_write(key)
    if stream_values is None:
      return

    stream_entry = store[key]

    if id_token == "*":
      entry_id = generate_stream_id(stream_entry)
    else:
      parsed_id = parse_stream_id(id_token)
      if parsed_id is None:
        return

      if parsed_id[1] == "*":
        entry_id = generate_stream_id(stream_entry, requested_ms=parsed_id[0])
      else:
        stream_entry["last_ms"], stream_entry["last_seq"] = parsed_id
        entry_id = id_token

    entry_fields = {}
    for i in range(0, len(field_values), 2):
      entry_fields[field_values[i]] = field_values[i + 1]

    stream_values.append({"id": entry_id, "fields": entry_fields})
    touch_key(key)
    return

  if command == "ZADD" and len(command_parts) >= 4:
    key = command_parts[1]
    score_members = command_parts[2:]
    if len(score_members) % 2 != 0:
      return

    entry = get_zset_entry_for_write(key)
    if entry is None:
      return

    values = entry["value"]
    for index in range(0, len(score_members), 2):
      try:
        score = float(score_members[index])
      except ValueError:
        return
      member = score_members[index + 1]
      values[member] = score

    touch_key(key)
    return

  if command == "ZREM" and len(command_parts) >= 3:
    key = command_parts[1]
    entry = get_zset_entry_for_read(key)
    if entry is None or entry == "WRONGTYPE":
      return

    removed = 0
    for member in command_parts[2:]:
      if member in entry["value"]:
        del entry["value"][member]
        entry["geo"].pop(member, None)
        removed += 1

    if removed > 0:
      touch_key(key)
      if len(entry["value"]) == 0:
        del store[key]
    return

  if command == "GEOADD" and len(command_parts) >= 5:
    key = command_parts[1]
    triples = command_parts[2:]
    if len(triples) % 3 != 0:
      return

    entry = get_zset_entry_for_write(key)
    if entry is None:
      return

    values = entry["value"]
    geo_values = entry["geo"]

    for index in range(0, len(triples), 3):
      try:
        lon = float(triples[index])
        lat = float(triples[index + 1])
      except ValueError:
        return
      member = triples[index + 2]

      if not validate_geo_coordinates(lon, lat):
        return

      values[member] = calculate_geo_score(lon, lat)
      geo_values[member] = (lon, lat)

    touch_key(key)
    return


def execute_command(connection, selector, command_parts, raw_command=None, send_response=True, from_master=False):
  command = command_parts[0].upper()

  if command == "AUTH":
    username = "default"
    password = None

    if len(command_parts) == 2:
      password = command_parts[1]
    elif len(command_parts) == 3:
      username = command_parts[1]
      password = command_parts[2]
    else:
      if send_response:
        connection.sendall(encode_error("ERR wrong number of arguments for 'auth' command"))
      return True

    if username != "default":
      if send_response:
        connection.sendall(encode_error("WRONGPASS invalid username-password pair or user is disabled."))
      return True

    if not default_user_password_hashes and default_user_nopass:
      if send_response:
        connection.sendall(encode_error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"))
      return True

    if hash_password(password) in default_user_password_hashes:
      authenticated_connections.add(connection)
      if send_response:
        connection.sendall(encode_simple_string("OK"))
      return True

    if send_response:
      connection.sendall(encode_error("WRONGPASS invalid username-password pair or user is disabled."))
    return True

  if command == "ACL" and len(command_parts) >= 2:
    subcommand = command_parts[1].upper()

    if subcommand == "WHOAMI":
      if send_response:
        connection.sendall(encode_bulk_string("default"))
      return True

    if subcommand == "GETUSER" and len(command_parts) >= 3:
      username = command_parts[2]
      if username != "default":
        if send_response:
          connection.sendall(encode_bulk_string(None))
        return True

      if send_response:
        connection.sendall(encode_array(default_user_acl_response()))
      return True

    if subcommand == "SETUSER" and len(command_parts) >= 3:
      username = command_parts[2]
      if username != "default":
        if send_response:
          connection.sendall(encode_error("ERR ACL SETUSER only supports the default user"))
        return True

      result = apply_acl_setuser_default(connection, command_parts[3:])
      if send_response:
        if isinstance(result, dict) and result.get("type") == "error":
          connection.sendall(encode_error(result["message"]))
        else:
          connection.sendall(encode_simple_string("OK"))
      return True

    if send_response:
      connection.sendall(encode_error("ERR unknown subcommand or wrong number of arguments for ACL"))
    return True

  if command == "REPLCONF":
    if len(command_parts) >= 3 and command_parts[1].upper() == "GETACK":
      if role == "slave" and master_connection is not None:
        ack_payload = encode_array(["REPLCONF", "ACK", str(replica_processed_offset)])
        try:
          master_connection.sendall(ack_payload)
        except OSError:
          pass
      if send_response:
        connection.sendall(encode_simple_string("OK"))
      return True

    if len(command_parts) >= 3 and command_parts[1].upper() == "ACK":
      try:
        replica_ack_offsets[connection] = int(command_parts[2])
      except ValueError:
        replica_ack_offsets[connection] = 0
      return True

    if send_response:
      connection.sendall(encode_simple_string("OK"))
    return True

  if command == "PSYNC":
    if send_response:
      connection.sendall(encode_simple_string(f"FULLRESYNC {master_replid} {master_repl_offset}"))
      rdb_payload = bytes.fromhex(EMPTY_RDB_HEX)
      connection.sendall(f"${len(rdb_payload)}\r\n".encode() + rdb_payload)

    replica_connections.add(connection)
    replica_ack_offsets[connection] = master_repl_offset
    return True

  if command == "WAIT":
    if send_response:
      handle_wait(connection, selector, command_parts)
    return True

  if command == "CONFIG" and len(command_parts) >= 3 and command_parts[1].upper() == "GET":
    requested_name = command_parts[2].lower()
    if requested_name == "dir":
      if send_response:
        connection.sendall(encode_array(["dir", rdb_dir]))
      return True

    if requested_name == "dbfilename":
      if send_response:
        connection.sendall(encode_array(["dbfilename", dbfilename]))
      return True

    if requested_name == "appendonly":
      if send_response:
        connection.sendall(encode_array(["appendonly", "yes" if appendonly_enabled else "no"]))
      return True

    if requested_name == "appenddirname":
      if send_response:
        connection.sendall(encode_array(["appenddirname", appenddirname]))
      return True

    if requested_name == "appendfilename":
      if send_response:
        connection.sendall(encode_array(["appendfilename", appendfilename]))
      return True

    if send_response:
      connection.sendall(encode_array([]))
    return True

  if command == "INFO":
    if len(command_parts) >= 2 and command_parts[1].lower() == "replication":
      if send_response:
        connection.sendall(encode_bulk_string(replication_info_text()))
      return True

  if command == "KEYS" and len(command_parts) >= 2:
    pattern = command_parts[1]
    keys = []

    for key in list(store.keys()):
      if get_entry(key) is None:
        continue

      if pattern == "*" or fnmatch.fnmatch(key, pattern):
        keys.append(key)

    keys.sort()

    if send_response:
      connection.sendall(encode_array(keys))
    return True

  if command == "PUBLISH" and len(command_parts) >= 3:
    delivered = publish_message(command_parts[1], command_parts[2])
    if send_response:
      connection.sendall(encode_integer(delivered))
    return True

  if command == "ZADD" and len(command_parts) >= 4:
    key = command_parts[1]
    score_members = command_parts[2:]
    if len(score_members) % 2 != 0:
      if send_response:
        connection.sendall(encode_error("ERR syntax error"))
      return True

    entry = get_zset_entry_for_write(key)
    if entry is None:
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    values = entry["value"]
    added = 0
    for index in range(0, len(score_members), 2):
      score = float(score_members[index])
      member = score_members[index + 1]
      if member not in values:
        added += 1
      values[member] = score

    if added > 0 or score_members:
      touch_key(key)

    replicate_and_persist(raw_command, command)

    if send_response:
      connection.sendall(encode_integer(added))
    return True

  if command == "ZRANK" and len(command_parts) >= 3:
    entry = get_zset_entry_for_read(command_parts[1])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_bulk_string(None))
      return True

    member = command_parts[2]
    items = zset_sorted_items(entry["value"])
    rank = None
    for index, (name, _) in enumerate(items):
      if name == member:
        rank = index
        break

    if send_response:
      if rank is None:
        connection.sendall(encode_bulk_string(None))
      else:
        connection.sendall(encode_integer(rank))
    return True

  if command == "ZRANGE" and len(command_parts) >= 4:
    entry = get_zset_entry_for_read(command_parts[1])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_array([]))
      return True

    start = int(command_parts[2])
    stop = int(command_parts[3])
    withscores = len(command_parts) >= 5 and command_parts[4].upper() == "WITHSCORES"

    items = zset_sorted_items(entry["value"])
    members = [member for member, _ in items]
    selected = trim_lrange(members, start, stop)

    if withscores:
      result = []
      for member in selected:
        result.append(member)
        result.append(format_float(entry["value"][member]))
    else:
      result = selected

    if send_response:
      connection.sendall(encode_array(result))
    return True

  if command == "ZCARD" and len(command_parts) >= 2:
    entry = get_zset_entry_for_read(command_parts[1])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    count = 0 if entry is None else len(entry["value"])
    if send_response:
      connection.sendall(encode_integer(count))
    return True

  if command == "ZSCORE" and len(command_parts) >= 3:
    entry = get_zset_entry_for_read(command_parts[1])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_bulk_string(None))
      return True

    member = command_parts[2]
    score = entry["value"].get(member)
    if send_response:
      if score is None:
        connection.sendall(encode_bulk_string(None))
      else:
        connection.sendall(encode_bulk_string(format_float(score)))
    return True

  if command == "ZREM" and len(command_parts) >= 3:
    key = command_parts[1]
    entry = get_zset_entry_for_read(key)
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_integer(0))
      return True

    removed = 0
    for member in command_parts[2:]:
      if member in entry["value"]:
        del entry["value"][member]
        entry["geo"].pop(member, None)
        removed += 1

    if removed > 0:
      touch_key(key)
      if len(entry["value"]) == 0:
        del store[key]

      replicate_and_persist(raw_command, command)

    if send_response:
      connection.sendall(encode_integer(removed))
    return True

  if command == "GEOADD" and len(command_parts) >= 5:
    key = command_parts[1]
    triples = command_parts[2:]
    if len(triples) % 3 != 0:
      if send_response:
        connection.sendall(encode_error("ERR syntax error"))
      return True

    entry = get_zset_entry_for_write(key)
    if entry is None:
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    values = entry["value"]
    geo_values = entry["geo"]
    added = 0

    for index in range(0, len(triples), 3):
      lon = float(triples[index])
      lat = float(triples[index + 1])
      member = triples[index + 2]

      if not validate_geo_coordinates(lon, lat):
        if send_response:
          connection.sendall(encode_error("ERR invalid longitude,latitude pair"))
        return True

      if member not in values:
        added += 1

      values[member] = calculate_geo_score(lon, lat)
      geo_values[member] = (lon, lat)

    touch_key(key)
    replicate_and_persist(raw_command, command)
    if send_response:
      connection.sendall(encode_integer(added))
    return True

  if command == "GEOPOS" and len(command_parts) >= 3:
    entry = get_zset_entry_for_read(command_parts[1])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_array([make_null_array_value() for _ in command_parts[2:]]))
      return True

    response = []
    for member in command_parts[2:]:
      coordinates = get_geo_coordinates(entry, member)

      if coordinates is None:
        response.append(make_null_array_value())
      else:
        lon, lat = coordinates
        response.append([format_float(lon), format_float(lat)])

    if send_response:
      connection.sendall(encode_array(response))
    return True

  if command == "GEODIST" and len(command_parts) >= 4:
    entry = get_zset_entry_for_read(command_parts[1])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_bulk_string(None))
      return True

    unit = "m"
    if len(command_parts) >= 5:
      unit = command_parts[4]

    multiplier = unit_to_meters_multiplier(unit)
    if multiplier is None:
      if send_response:
        connection.sendall(encode_error("ERR unsupported unit provided. please use m, km, ft, mi"))
      return True

    member1 = get_geo_coordinates(entry, command_parts[2])
    member2 = get_geo_coordinates(entry, command_parts[3])
    if member1 is None or member2 is None:
      if send_response:
        connection.sendall(encode_bulk_string(None))
      return True

    distance = geo_distance_meters(member1[0], member1[1], member2[0], member2[1]) / multiplier
    if send_response:
      connection.sendall(encode_bulk_string(format_float(distance)))
    return True

  if command == "GEOSEARCH":
    query = parse_geosearch_arguments(command_parts)
    if query is None:
      if send_response:
        connection.sendall(encode_error("ERR syntax error"))
      return True

    entry = get_zset_entry_for_read(query["key"])
    if entry == "WRONGTYPE":
      if send_response:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return True

    if entry is None:
      if send_response:
        connection.sendall(encode_array([]))
      return True

    result = run_geosearch(query, entry)
    if result == "ERR":
      if send_response:
        connection.sendall(encode_error("ERR unsupported unit provided. please use m, km, ft, mi"))
      return True

    if send_response:
      connection.sendall(encode_array(result))
    return True

  if command == "GEORADIUS" and len(command_parts) >= 6:
    translated = [
      "GEOSEARCH",
      command_parts[1],
      "FROMLONLAT",
      command_parts[2],
      command_parts[3],
      "BYRADIUS",
      command_parts[4],
      command_parts[5],
    ] + command_parts[6:]
    return execute_command(connection, selector, translated, raw_command, send_response, from_master)

  if command == "PING":
    if send_response:
      connection.sendall(encode_simple_string("PONG"))
    return True

  return False
  
def read_client(connection, selector):
  try:
    data = connection.recv(1024)
  except ConnectionResetError:
    close_client(connection, selector)
    return
  
  if not data:
    close_client(connection, selector)
    return

  commands = extract_resp_commands(connection, data)
  if not commands:
    command_parts = parse_resp_command(data)
    if command_parts:
      commands = [(command_parts, data)]
    else:
      connection.sendall(encode_simple_string("PONG"))
      return

  for command_parts, raw_command in commands:
    if not command_parts:
      continue

    command = command_parts[0].upper()

    if not is_connection_authenticated(connection) and command != "AUTH":
      connection.sendall(encode_error("NOAUTH Authentication required."))
      continue

    in_subscribed_mode = get_subscription_count(connection) > 0

    if in_subscribed_mode and command not in ("SUBSCRIBE", "UNSUBSCRIBE", "PING"):
      connection.sendall(encode_error(f"ERR can't execute '{command.lower()}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"))
      continue

    if command == "SUBSCRIBE":
      channels = command_parts[1:]
      for channel in channels:
        count = subscribe_connection_to_channel(connection, channel)
        connection.sendall(encode_array(["subscribe", channel, count]))
      continue

    if command == "UNSUBSCRIBE":
      channels = command_parts[1:]
      if channels:
        for channel in channels:
          count = unsubscribe_connection_from_channel(connection, channel)
          connection.sendall(encode_array(["unsubscribe", channel, count]))
      else:
        subscribed_channels = unsubscribe_all_channels(connection)
        if not subscribed_channels:
          connection.sendall(encode_array(["unsubscribe", None, 0]))
        else:
          remaining = len(subscribed_channels)
          for channel in subscribed_channels:
            remaining -= 1
            connection.sendall(encode_array(["unsubscribe", channel, remaining]))
      continue

    if command == "PING" and in_subscribed_mode:
      pong_message = ""
      if len(command_parts) >= 2:
        pong_message = command_parts[1]
      connection.sendall(encode_array(["pong", pong_message]))
      continue

    if execute_command(connection, selector, command_parts, raw_command=raw_command, send_response=True):
      continue

    if command == "WATCH":
      if is_transaction_active(connection):
        connection.sendall(encode_error("ERR WATCH inside MULTI is not allowed"))
        continue
      watch_keys_for_connection(connection, command_parts[1:])
      connection.sendall(encode_simple_string("OK"))
      continue

    if command == "UNWATCH":
      if is_transaction_active(connection):
        connection.sendall(encode_error("ERR UNWATCH inside MULTI is not allowed"))
        continue
      clear_watched_keys(connection)
      connection.sendall(encode_simple_string("OK"))
      continue

    if is_transaction_active(connection):
      if command == "EXEC":
        execute_transaction_queue(connection, selector)
        continue

      if command == "DISCARD":
        clear_transaction(connection)
        clear_watched_keys(connection)
        connection.sendall(encode_simple_string("OK"))
        continue

      if command == "MULTI":
        connection.sendall(encode_error("ERR MULTI calls can not be nested"))
        continue

      get_transaction_queue(connection).append(command_parts)
      connection.sendall(encode_simple_string("QUEUED"))
      continue

    if command == "MULTI":
      get_transaction_queue(connection)
      connection.sendall(encode_simple_string("OK"))
      continue

    if command == "EXEC":
      connection.sendall(encode_error("ERR EXEC without MULTI"))
      continue

    if command == "DISCARD":
      connection.sendall(encode_error("ERR DISCARD without MULTI"))
      continue
    
    if command == "ECHO" and len(command_parts) >= 2:
      connection.sendall(encode_bulk_string(command_parts[1]))
      continue
    
    if command == "SET" and len(command_parts) >= 3:
      key = command_parts[1]
      value = command_parts[2]

      expires_at = None
      if len(command_parts) >= 5:
        option = command_parts[3].upper()
        option_value = command_parts[4]

        if option == "PX":
          expires_at = time.monotonic() + (int(option_value) / 1000)
        elif option == "EX":
          expires_at = time.monotonic() + int(option_value)

      store[key] = make_string_entry(value, expires_at)
      touch_key(key)
      replicate_and_persist(raw_command, command)
      connection.sendall(encode_simple_string("OK"))
      continue

    if command == "GET" and len(command_parts) >= 2:
      key = command_parts[1]
      connection.sendall(encode_bulk_string(get_value(key)))
      continue

    if command == "INCR" and len(command_parts) >= 2:
      result = apply_incr(command_parts[1])
      if isinstance(result, dict) and result.get("type") == "error":
        connection.sendall(encode_error(result["message"]))
        continue

      replicate_and_persist(raw_command, command)
      connection.sendall(encode_integer(result))
      continue
    
    if command == "TYPE" and len(command_parts) >= 2:
      key = command_parts[1]
      entry = get_entry(key)
      
      if entry is None:
        connection.sendall(encode_simple_string("none"))
        continue
      
      connection.sendall(encode_simple_string(entry["type"]))
      continue
    
    if command == "XADD" and len(command_parts) >= 5:
      key = command_parts[1]
      id_token = command_parts[2]
      field_values = command_parts[3:]

      if len(field_values) % 2 != 0:
        connection.sendall(encode_error("ERR wrong number of arguments for 'XADD' command"))
        continue

      stream_values = get_stream_for_write(key)
      if stream_values is None:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      stream_entry = store[key]

      if id_token == "*":
        entry_id = generate_stream_id(stream_entry)
      else:
        parsed_id = parse_stream_id(id_token)
        if parsed_id is None:
          connection.sendall(encode_error("ERR Invalid stream ID specified as stream command argument"))
          continue

        if parsed_id[1] == "*":
          requested_ms = parsed_id[0]
          if requested_ms < stream_entry["last_ms"]:
            connection.sendall(encode_error("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
            continue

          entry_id = generate_stream_id(stream_entry, requested_ms=requested_ms)
        else:
          if parsed_id <= (0, 0):
            connection.sendall(encode_error("ERR The ID specified in XADD must be greater than 0-0"))
            continue

          last_stream_id = (0, 0)
          if stream_values:
            last_stream_id = parse_stream_id(stream_values[-1]["id"])
            if last_stream_id is None:
              connection.sendall(encode_error("ERR Invalid stream state"))
              continue

          if parsed_id <= last_stream_id:
            connection.sendall(encode_error("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
            continue

          stream_entry["last_ms"], stream_entry["last_seq"] = parsed_id
          entry_id = id_token

      entry_fields = {}
      for i in range(0, len(field_values), 2):
        entry_fields[field_values[i]] = field_values[i + 1]

      stream_values.append({
        "id": entry_id,
        "fields": entry_fields,
      })

      touch_key(key)

      replicate_and_persist(raw_command, command)
      connection.sendall(encode_bulk_string(entry_id))
      wake_pending_xread_requests()
      continue

    if command == "XRANGE" and len(command_parts) >= 4:
      key = command_parts[1]
      min_token = command_parts[2]
      max_token = command_parts[3]

      entry = get_entry(key)
      if entry is None:
        connection.sendall(encode_array([]))
        continue

      if entry["type"] != "stream":
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      if min_token == "-":
        min_id = (-1, -1)
      else:
        min_id = parse_stream_id(min_token)
        if min_id is None or min_id == "+":
          connection.sendall(encode_error("ERR Invalid stream ID specified as stream command argument"))
          continue

      if max_token == "+":
        max_id = (10**30, 10**30)
      else:
        max_id = parse_stream_id(max_token)
        if max_id is None or max_id == "-":
          connection.sendall(encode_error("ERR Invalid stream ID specified as stream command argument"))
          continue

      result = []
      for stream_item in entry["value"]:
        item_id = parse_stream_id(stream_item["id"])
        if item_id is None:
          continue

        if min_id <= item_id <= max_id:
          fields = []
          for field_name, field_value in stream_item["fields"].items():
            fields.append(field_name)
            fields.append(field_value)

          result.append([stream_item["id"], fields])

      connection.sendall(encode_array(result))
      continue

    if command == "XREAD":
      parsed = parse_xread_command(command_parts)
      if parsed is None:
        connection.sendall(encode_error("ERR Invalid stream ID specified as stream command argument"))
        continue

      if parsed == "WRONGTYPE":
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      response = build_xread_response(parsed["keys"], parsed["cursors"])
      if response is not None:
        connection.sendall(encode_array(response))
        continue

      if parsed["block_timeout"] is None:
        connection.sendall(encode_array(None))
        continue

      deadline = None
      if parsed["block_timeout"] > 0:
        deadline = time.monotonic() + (parsed["block_timeout"] / 1000)

      pending_xread_requests.append({
        "connection": connection,
        "keys": parsed["keys"],
        "cursors": parsed["cursors"],
        "deadline": deadline,
      })
      continue
      
    if command == "RPUSH" and len(command_parts) >= 3:
      key = command_parts[1]
      values = command_parts[2:]

      list_values = get_list_for_write(key)
      if list_values is None:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      list_values.extend(values)
      touch_key(key)
      replicate_and_persist(raw_command, command)
      connection.sendall(encode_integer(len(list_values)))
      wake_pending_blpop_requests()
      continue

    if command == "LPUSH" and len(command_parts) >= 3:
      key = command_parts[1]
      values = command_parts[2:]

      list_values = get_list_for_write(key)
      if list_values is None:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      for value in values:
        list_values.insert(0, value)

      touch_key(key)
      replicate_and_persist(raw_command, command)
      connection.sendall(encode_integer(len(list_values)))
      wake_pending_blpop_requests()
      continue

    if command == "LRANGE" and len(command_parts) >= 4:
      key = command_parts[1]
      start = int(command_parts[2])
      stop = int(command_parts[3])

      list_values = get_list_for_read(key)
      if list_values is None:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      connection.sendall(encode_array(trim_lrange(list_values, start, stop)))
      continue

    if command == "LLEN" and len(command_parts) >= 2:
      key = command_parts[1]
      list_values = get_list_for_read(key)

      if list_values is None:
        connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
        continue

      connection.sendall(encode_integer(len(list_values)))
      continue

    if command == "LPOP" and len(command_parts) >= 2:
      key = command_parts[1]

      if len(command_parts) >= 3:
        count = int(command_parts[2])
        popped_items = pop_from_list(key, count)

        if popped_items is None:
          connection.sendall(encode_array(None))
          continue

        replicate_and_persist(raw_command, command)
        connection.sendall(encode_array(popped_items))
        continue

      popped_item = pop_from_list(key)
      if popped_item is not None:
        replicate_and_persist(raw_command, command)
      connection.sendall(encode_bulk_string(popped_item))
      continue

    if command == "BLPOP" and len(command_parts) >= 3:
      keys = command_parts[1:-1]
      timeout_seconds = float(command_parts[-1])
      response = try_pop_from_keys(keys)
      if response is not None:
        connection.sendall(encode_array(response))
        continue

      deadline = None
      if timeout_seconds > 0:
        deadline = time.monotonic() + timeout_seconds

      pending_blpop_requests.append({
        "connection": connection,
        "keys": keys,
        "deadline": deadline,
      })
      return
    connection.sendall(encode_simple_string("OK"))


def read_master(connection, selector):
  try:
    data = connection.recv(4096)
  except ConnectionResetError:
    close_client(connection, selector)
    return

  if not data:
    close_client(connection, selector)
    return

  commands = extract_resp_commands(connection, data)
  process_master_commands(connection, selector, commands)

def close_client(connection, selector):
  remove_pending_requests_for_connection(connection)

  try:
    selector.unregister(connection)
  except Exception:
    pass
  
  connection.close()

def main():
    print("Logs from your program will appear here!")

    global master_replid
    parse_server_config(sys.argv)
    master_replid = random_replid()
    load_rdb_file()
    initialize_aof_storage()
    replay_aof_if_enabled()

    selector = selectors.DefaultSelector()
    
    server_socket = socket.create_server(("localhost", listen_port), reuse_port=(sys.platform != "win32"))
    server_socket.setblocking(False)
    
    selector.register(server_socket, selectors.EVENT_READ, accept_connection)

    if role == "slave":
      connect_to_master_and_handshake(selector)
    
    while True:
      events = selector.select(timeout=get_selector_timeout())
      for key, _ in events:
        callback = key.data
        callback(key.fileobj, selector)

      expire_pending_blpop_requests()
      expire_pending_xread_requests()

if __name__ == "__main__":
    main()
