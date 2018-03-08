type program_status = Edition of string (* last error *)
                    | Compiling | Compiled | Running | Stopping
                    [@@ppp PPP_JSON]

(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type. TAny is means to be replaced by an actual type during compilation:
 * all TAny types in an expression will be changed to a specific type that's
 * large enought to accommodate all the values at hand. *)
type scalar_typ =
  | TNull | TFloat | TString | TBool | TNum | TAny
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TCidrv4 | TCidrv6 [@@ppp PPP_JSON]

type time_range =
  NoData | TimeRange of float * float [@@ppp PPP_JSON]
