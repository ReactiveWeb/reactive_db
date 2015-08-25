defmodule Reactive.MapsDb do
  defmodule MapRef do
    defstruct db: :null, map_id: :null
  end
  def create(db,map_id) do
    ### TODO: Add begin - end
    Reactive.Db.put(db,map_id <> ":a","map begin")
    Reactive.Db.put(db,map_id <> ":z","map end")

    %MapRef{ db: db, map_id: map_id }
  end
  def remove(mapRef=%{ db: db, map_id: map_id}) do
    raise "not implemented"
  end
  def put(mapRef=%{ db: db, map_id: map_id},key,value) do
    Reactive.Db.put(db,map_id <> ":m:" <> key,:erlang.term_to_binary(value))
  end
  def get(mapRef=%{ db: db, map_id: map_id},key,default \\ :not_found) do
    case Reactive.Db.get(db,map_id <> ":m:" <> key) do
      :not_found -> :not_found
      value -> :erlang.binary_to_term(value)
    end
  end
  def delete(mapRef=%{ db: db, map_id: map_id},key) do
    Reactive.Db.delete(db,map_id <> ":m:" <> key)
  end

end