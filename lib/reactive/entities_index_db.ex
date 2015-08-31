defmodule Reactive.EntitiesIndexDb do
  require Logger

  def create({db,index_name}) do
    ### TODO: Add begin - end
    Reactive.Db.put(db,"ei:" <> index_name <> ":a","entity index begin")
    Reactive.Db.put(db,"ei:" <> index_name <> ":z","entity index end")
    {db,index_name}
  end
  def remove(mapRef=%{ db: db, map_id: map_id}) do
    raise "not implemented"
  end
  def add({db,index_name},key,entity_id) do
    Logger.debug("Index #{index_name} ADD #{key} #{inspect entity_id}")
    dbId=Reactive.EntitiesDb.entity_db_id(entity_id)
    keyPrefix="ei:" <> index_name <> ":i:" <> key <> ":"
    Reactive.Db.put(db,keyPrefix <> dbId,:erlang.term_to_binary(entity_id))
  end
  def find({db,index_name},key) do
    keyPrefix="ei:" <> index_name <> ":i:" <> key <> ":"
    values=Reactive.Db.scan(db,%{
      prefix: keyPrefix,
      fetch: :value
    })
    results=Enum.map(values,fn(b) -> :erlang.binary_to_term(b) end)
    Logger.debug("Index #{index_name} FIND #{key} -> #{inspect results}")
    results
  end
  def delete({db,index_name},key,entity_id) do
    Logger.debug("Index #{index_name} DELETE #{key} #{inspect entity_id}")
    dbId=Reactive.EntitiesDb.entity_db_id(entity_id)
    keyPrefix="ei:" <> index_name <> ":i:" <> key <> ":"
    Reactive.Db.delete(db,keyPrefix <> dbId)
  end
end