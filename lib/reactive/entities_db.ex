defmodule Reactive.EntitiesDb do

  defp argToBinary(arg) when is_binary(arg) do
    [arg]
  end
  defp argToBinary(arg) when is_atom(arg) do
    ["@",:erlang.atom_to_binary(arg,:utf8)]
  end
  # defp argToBinary(arg) when is_list(arg) do
  #   ["[", Enum.map( arg , fn ( x ) ->  [ argToBinary( x ), ","] end ), "]"]
  # end
  defp argToBinary(arg) when is_tuple(arg) do
    ["{", Enum.map( :erlang.tuple_to_list(arg) , fn ( x ) ->  [ argToBinary( x ), "," ] end ), "}"]
  end
  defp argToBinary(arg) do
    arg
  end

  def entity_db_id(_id=[module | args]) do
    #IO.inspect(args)
    argss = Enum.map( args , fn ( x ) ->  [ argToBinary( x ), ","] end )
    :erlang.iolist_to_binary( ["e:",:erlang.atom_to_binary(module,:utf8),":",argss ] )
  end

  def store(db,id,entityData) do
    sdata=:erlang.term_to_binary(entityData)
    Reactive.Db.put(db,entity_db_id(id),sdata)
  end

  def retrive(db,id) do
    case Reactive.Db.get(db,entity_db_id(id)) do
      {:ok, binary} -> {:ok,:erlang.binary_to_term(binary)}
      not_found -> not_found
    end
  end

end