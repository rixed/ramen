(* The configuration tree is a map of some keys into tome values, where the keys
 * can also be considered as a hierarchy.
 *
 * The API we need for the configuration service is:
 * - Add a new binding, which overwrite any previous one;
 * - Remove a binding by key;
 * - Check if a key is bound;
 * - Lookup a key;
 * - Iterate / fold over all or a subset of the bindings, in read only mode;
 * - Iterate / fold over all or a subset of the bindings, allowing additions
 *   and deletions of any keys in the process;
 *
 * A subsets of the bindings is defined by a key prefix (as string).
 * Therefore keys must be convertible to/from strings.
 *)

module type KEY =
sig
  type t
  val of_string : string -> t
  val to_string : t -> string
  val print : 'a BatIO.output -> t -> unit
  val equal : t -> t -> bool
  val hash : t -> int
end

module type TREE =
sig
  module Key : KEY
  type 'a t

  (* Signature suggest a mutable implementation (despite the first
   * implementation below is actually a stateful hash) *)
  val empty : 'a t
  val add : Key.t -> 'a -> 'a t -> 'a t
  val rem : Key.t -> 'a t -> 'a t

  val length : 'a t -> int
  val mem : 'a t -> Key.t -> bool
  val get : 'a t -> Key.t -> 'a
  val fold :
    'a t -> ?prefix:string -> (Key.t -> 'a -> 'u -> 'u) -> 'u -> 'u
  val fold_safe :
    'a t -> ?prefix:string -> (Key.t -> 'a -> 'u -> 'u) -> 'u -> 'u
end

module Impl =
struct
  (* Simple implementation of this: just use a hash! *)
  module Hash (Key : KEY) : TREE with module Key = Key =
  struct
    open Batteries
    module Key = Key
    module H = Hashtbl.Make (Key)
    type 'a t = 'a H.t option

    let empty = None

    let add k v = function
      | None ->
          let h = H.create 99 in
          H.add h k v ;
          Some h
      | Some h as t ->
          H.replace h k v ;
          t

    let rem k = function
      | None ->
          raise Not_found
      | Some h as t ->
          H.remove h k ;
          t

    let length = function
      | None -> 0
      | Some h -> H.length h

    let mem t k =
      match t with
      | None -> false
      | Some h -> H.mem h k

    let get t k =
      match t with
      | None ->
          raise Not_found
      | Some h ->
          H.find h k

    let fold t ?prefix f u =
      (* Sadly, as turning all keys into strings would slow us down.
       * That's why we have the PrefixTree implementation below! *)
      ignore prefix ;
      match t with
      | None ->
          u
      | Some h ->
          H.fold f h u

    let fold_safe t ?prefix f u =
      ignore prefix ;
      match t with
      | None ->
          u
      | Some h ->
          (* Materialize the enum in a local array to allow modification of the hash
           * by the callback function [f]: *)
          let keys = H.keys h |> Array.of_enum in
          Array.fold_left (fun u k ->
            match H.find h k with
            | exception Not_found -> u (* This key has been deleted *)
            | v -> f k v u
          ) u keys
  end

  (* Another implementation based on a prefix tree of the keys *)
  module PrefixTree (Key : KEY) : TREE with module Key = Key =
  struct
    module Key = Key
    module StringKey = Prefix_tree.StringKey
    module T = Prefix_tree.Make (StringKey)

    (* Also store the key in symbolic form: *)
    type 'a t = (Key.t * 'a) T.t

    let empty = T.empty

    let add k v t =
      let ks = Key.to_string k in
      T.add ks (k, v) t

    let rem k t =
      let ks = Key.to_string k in
      T.remove ks t

    let length = T.length

    let get t k =
      let ks = Key.to_string k in
      T.lookup t ks |> snd

    let mem t k =
      match get t k with
      | exception Not_found -> false
      | _ -> true

    let fold t ?prefix f u =
      T.fold t ?prefix (fun _ks (k, v) u ->
        f k v u
      ) u

    (* Since the Prefix_tree is persistent then [fold] is already safe: *)
    let fold_safe = fold
  end
end
