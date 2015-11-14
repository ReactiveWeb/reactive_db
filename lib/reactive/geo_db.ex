defmodule Reactive.GeoDb do
  use Bitwise
  require Logger

  def lla_to_ecef({lat, lon, alt}) do
    rad = 6378137.0        # Radius of the Earth (in meters)
    f = 1.0/298.257223563  # Flattening factor WGS84 Model
    cosLat = :math.cos(lat)
    sinLat = :math.sin(lat)
    oneMinusF = (1.0-f)
    ff = oneMinusF*oneMinusF
    c = 1/ :math.sqrt(cosLat*cosLat + ff * sinLat*cosLat)
    s = c * ff

    cosLon = :math.cos(lon)
    x = (rad * c + alt)*cosLat * cosLon
    y = (rad * c + alt)*cosLat * cosLon
    z = (rad * s + alt)*sinLat

    {x, y, z}
  end

  def octree_hash({x,y,z}) do
    a=8000000
    {ix,iy,iz}={round(x+a),round(y+a),round(z+a)}
    {bx,by,bz}={<< ix :: size(24) >>,<< iy :: size(24) >>,<< iz :: size(24) >>}
    mix_bits(bx,by,bz)
  end

  def mix_bits(x,y,z) do
    mix_bits(x,y,z,"")
  end
  defp mix_bits("","","",acc) do
    acc
  end
  defp mix_bits(x,y,z,acc) do
    << xb :: size(1), xr :: bitstring >> = x
    << yb :: size(1), yr :: bitstring >> = y
    << zb :: size(1), zr :: bitstring >> = z
    mix_bits(xr,yr,zr,<<acc :: bitstring, xb :: size(1), yb :: size(1), zb :: size(1)>>)
  end

  def cell_range(cell_id) do
    s = bit_size(cell_id)
    r = 72-s
    fill = :erlang.bsl(2,r)-1
  #  Logger.info("CE #{inspect r} #{inspect fill}")
    b = << cell_id :: bitstring, 0 :: size(r) >>
    e = << cell_id :: bitstring, fill :: size(r) >>
    {b,e}
  end

  def cells_to_search({xp,yp,zp},maxDist) do
    gbits = :erlang.trunc(:math.log(maxDist) / :math.log(2))+1
    step = 1 <<< gbits
    steps = :erlang.trunc(maxDist/step)+1
    #Logger.debug("GBITS: #{inspect gbits} STEP: #{inspect step} STEPS: #{inspect steps}")
    cells = for x <- -steps .. steps, y <- -steps .. steps, z <- -steps .. steps, do: {xp+x*step, yp+y*step, zp+z*step}
    fbits=72-gbits*3
    cell_ids = for cell <- cells do
      << cell_id :: bitstring-size(fbits), _ :: bitstring >> = octree_hash(cell)
      cell_id
    end
    {cells,cell_ids}
  end

  def distanceSq({x1,y1,z1},{x2,y2,z2}) do
    {xd,yd,zd}={x2-x1,y2-y1,z2-z1}
    xd*xd+yd*yd+zd*zd
  end

  defmodule GeoRef do
    defstruct db: :null, geo_id: :null
  end

  def create(db,geo_id) do
    ### TODO: Add begin - end
    Reactive.Db.put(db,geo_id <> ":a","geodb begin")
    Reactive.Db.put(db,geo_id <> ":z","geodb end")

    %GeoRef{ db: db, geo_id: geo_id }
  end
  def remove(geoRef=%{ db: db, geo_id: geo_id}) do
    raise "not implemented"
  end
  def put(geoRef=%{ db: db, geo_id: geo_id},lla,uniq,value) do
    pos = lla_to_ecef(lla)
    key = octree_hash(pos) <> ":" <> uniq
    Reactive.Db.put(db,geo_id <> ":g:" <> key,:erlang.term_to_binary(%{
      value: value,
      ecef: pos,
      lla: lla
    }))
  end
  def put_ecef(geoRef=%{ db: db, geo_id: geo_id},ecef,uniq,value) do
    key = octree_hash(ecef) <> ":" <> uniq

   # Logger.debug("PUT ECEF #{inspect key}")
    Reactive.Db.put(db,geo_id <> ":g:" <> key,:erlang.term_to_binary(%{
      value: value,
      ecef: ecef
    }))
  end
  def delete_ecef(geoRef=%{ db: db, geo_id: geo_id},ecef,uniq) do
    key = octree_hash(ecef) <> ":" <> uniq
   # Logger.debug("DELETE ECEF #{inspect key}")
    Reactive.Db.delete(db,geo_id <> ":g:" <> key)
  end
  def delete(geoRef=%{ db: db, geo_id: geo_id},lla,uniq) do
    pos = lla_to_ecef(lla)
    key = octree_hash(pos) <> ":" <> uniq
    Reactive.Db.delete(db,geo_id <> ":g:" <> key)
  end
  def put_cell_summary(geoRef=%{ db: db, geo_id: geo_id},cell_id,summary) do
    {b,e}=cell_range(cell_id)
    Reactive.Db.put(db,geo_id <> ":s:" <> b,:erlang.term_to_binary(summary))
  end
  def get_cell_summary(geoRef=%{ db: db, geo_id: geo_id},cell_id,summary) do
    {b,e}=cell_range(cell_id)
    case Reactive.Db.get(db,geo_id <> ":s:" <> b <> ">summary") do
      :not_found -> :not_found
      value -> :erlang.binary_to_term(value)
    end
  end
  def scan_cell(geoRef=%{ db: db, geo_id: geo_id},cell_id,limit \\ 10_000) do
    {b,e} = cell_range(cell_id)
   # Logger.debug("Scan #{inspect cell_id} range  #{inspect b} to #{inspect e}")
    scr=Reactive.Db.scan(db,%{
      :prefix => geo_id <> ":g:",
      :begin => b,
      :end => e,
      :limit => limit
    })
     lsr=:lists.filtermap(fn({fullKey,value}) ->
         case fullKey do
           << _position :: size(72), ":" , uniq :: binary >> ->
             {true,:erlang.binary_to_term(value)}
           _ -> false
         end
       end,scr)
   #  Logger.debug("Scan result: #{inspect lsr}")
     lsr
  end
  def find(geoRef=%{ db: db, geo_id: geo_id},pos,maxDist,limit \\ 10_000) do
    maxDistSq=maxDist*maxDist
    {_,cell_ids}=cells_to_search(pos,maxDist)
    for cell_id <- cell_ids, place <- scan_cell(geoRef,cell_id,limit), distanceSq(pos,place.ecef)<=maxDistSq, do: place
  end
end