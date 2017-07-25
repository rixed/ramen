module Stdint =
struct

  type uint8 = int
  type uint16 = int
  type uint32 = int
  type uint64 = int
  type uint128 = int
  type int8 = int
  type int16 = int
  type int128 = int
  module Uint8 = struct type t = int end
  module Uint16 = struct type t = int end

end
