defmodule GeoDbTest do
  use ExUnit.Case
  require Logger

  setup do
    :os.cmd('rm -Rf test_db')
    db = %Reactive.GeoDb.GeoRef{ db: Reactive.LocalDb.open("test_db"), geo_id: "1234" }
    {:ok, [db: db]}
  end

  def random_pos() do
    {:random.uniform(14000000)-7000000,:random.uniform(14000000)-7000000,:random.uniform(14000000)-7000000}
  end

  test "put one place, find it at same place and delete", context do
   db = context.db
    pos=random_pos()
    Reactive.GeoDb.put_ecef(db,pos,"1","1")
    res=Reactive.GeoDb.find(db,pos,10,10)
    assert Enum.count(res) == 1
    Reactive.GeoDb.delete_ecef(db,pos,"1")
    res=Reactive.GeoDb.find(db,pos,10,10)
    assert Enum.count(res) == 0
  end

  test "put one place, find it from small distance and delete", context do
    db = context.db
    pos=random_pos()
    {x,y,z} = pos
    npos={x+500, y+500, z}
    Reactive.GeoDb.put_ecef(db,pos,"1","1")
    res=Reactive.GeoDb.find(db,npos,1000,10)
    assert Enum.count(res) == 1
    Reactive.GeoDb.delete_ecef(db,pos,"1")
    res=Reactive.GeoDb.find(db,npos,1000,10)
    assert Enum.count(res) == 0
  end

  test "put one place, find it from great distance and delete", context do
    db = context.db
    pos=random_pos()
    {x,y,z} = pos
    npos={x-500000, y-500000, z-500000}
    Reactive.GeoDb.put_ecef(db,pos,"1","1")
    res=Reactive.GeoDb.find(db,npos,1000000,10)
    assert Enum.count(res) == 1
    Reactive.GeoDb.delete_ecef(db,pos,"1")
    res=Reactive.GeoDb.find(db,npos,1000000,10)
    assert Enum.count(res) == 0
  end

  @tag timeout: 10_000
  test "put and find some places", context do
    db = context.db
    places = for i <- 1 .. 10_000, do: {random_pos(),
      Integer.to_string(i)}
    for place={pos, id} <- places, do: Reactive.GeoDb.put_ecef(db,pos,id,place)

    for i <- 1 .. 1000 do
      maxDist = i*1000
      maxDistSq=maxDist*maxDist
      pos=random_pos()
      fcnt = Enum.count(places,fn({pp,x}) -> Reactive.GeoDb.distanceSq(pos,pp) <= maxDistSq end)
      {time,res}= :timer.tc(Reactive.GeoDb,:find,[db,pos,maxDist,10000])
      dbcnt = Enum.count(res)
      Logger.debug("Search in #{inspect maxDist} meters that found #{inspect dbcnt} of #{inspect fcnt} places took #{inspect time} µs")
      assert fcnt == dbcnt
    end
  end

end
