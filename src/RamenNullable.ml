(* Available both from ramen and workers *)
(* For state values, we must distinguish between None for "not
 * already initialized" from None for NULL.
 * So any nullable state (ie the state of a stateful function that
 * is nullable because it propagates nulls from its arguments) must be
 * dealt with directly from the code generator.
 * The other common case for have a null result from a stateful function
 * is when it is using skip nulls and all its inputs have been thus skipped.
 * This case is also dealt with from the code generator.
 *
 * The only case when a statefull function might deal with Nulls is when
 * it returns Null for other reasons. In that case it would actually return
 * some special value, typically None, and the code generator will translate
 * this case also into Null.
 *)

type 'a nullable = Null | NotNull of 'a

let nullable_map f = function
  | Null -> Null
  | NotNull x -> NotNull (f x)

let nullable_get = function
  | Null -> invalid_arg "Nullable.get"
  | NotNull x -> x

let (|!) a b =
  match a with Null -> b | NotNull a -> a

let default d = function
  | Null -> d
  | NotNull x -> x

let default_delayed f = function
  | Null -> f ()
  | NotNull x -> x

let nullable_of_option = function
  | None -> Null
  | Some x -> NotNull x

let option_of_nullable = function
  | Null -> None
  | NotNull x -> Some x
