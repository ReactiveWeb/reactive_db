defmodule Reactive.EntitiesDb do

  def entity_db_id(_id=[module | args]) do
    #IO.inspect(args)
    argss = Enum.map( args , fn ( x ) ->  [ :erlang.term_to_binary( x ), ","] end )
    :erlang.iolist_to_binary( ["e:",:erlang.atom_to_list(module),":",argss ] )
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