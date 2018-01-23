defmodule Aggex do
  @moduledoc """
    Map Aggregation 

    ## Examples

    iex> Aggex.group_by( Aggex.test_data, ["year","team"], ["homerun", "hit"] )

    [{%{team: "(デ)", year: 2015}, %{hit: 570, homerun: 75}},
     {%{team: "(ヤ)", year: 2015}, %{hit: 664, homerun: 80}},
     {%{team: "(中)", year: 2015}, %{hit: 566, homerun: 38}},
     {%{team: "(巨)", year: 2015}, %{hit: 238, homerun: 27}},
     {%{team: "(広)", year: 2015}, %{hit: 409, homerun: 23}},
     {%{team: "(神)", year: 2015}, %{hit: 680, homerun: 56}}]

  """

  defp init(dataset, group_keys, summary_cols) do
    {first_key, _v} = Enum.at(dataset, 0) |> Enum.at(0)
    ds_atom_key? = is_atom(first_key)
    grp_keys = qry_keys(group_keys,   ds_atom_key?)
    sum_cols = qry_keys(summary_cols, ds_atom_key?)
    { grp_keys, sum_cols }
  end
  @doc """
    qry_keys
      string list convert to keyword list.
      ["year", "depart"] -> [:year, :depart"]

  """
  def qry_keys(group_keys, to_atom? \\ false) do
    case to_atom? do
       true -> Enum.map(group_keys, &String.to_atom(&1))
       _    -> group_keys
    end
  end
  @doc """
  
   group_by(json, ["year","staff_id","prod_id"], ["sales", "amount"])
  
  """
  def group_by(dataset, group_keys, summary_cols) do
    {grp_keys, sum_cols} = init(dataset, group_keys, summary_cols)

    Enum.group_by( dataset, fn r -> Map.take(r, grp_keys) end,
                            fn r -> Map.take(r, sum_cols) end) 
    |> Enum.map( fn {k, v} ->
                    { k , Enum.reduce(v, %{}, &intersect(&1, &2, sum_cols)) } end )
  end 
 
  #
  #    In the two maps, add the same key values contained in grp_keys and return a new Map
  #
  #    map2 : empty map available
  #
  defp intersect(map1,  map2, grp_keys) do
    Enum.reduce( map1, %{},
      fn ({key, v}, acc) ->
        case Enum.member?(grp_keys, key) && Map.size(map2) > 0 do
          true ->  new_v = v + map2[key]
                   Map.update(acc, key, new_v, &(&1 + new_v) ) 
          _   -> Map.put(acc, key, v )
        end
    end)
  end

  @doc """

   g_by(json, ["year","staff_id","prod_id"], ["sales", "amount"])
   Map.group_by non-use version

  """
  def g_by(dataset, group_keys, summary_cols ) do

    {grp_keys, sum_cols}= init(dataset, group_keys, summary_cols)
    Enum.reduce( dataset, %{}, fn (data_rec, acc_rec) -> 
      # 
      # genarating a following map
      # %{ map_key => %{ << - summary set - : inner_map >> %} }
      # 
      map_key =  Map.take(data_rec, grp_keys)

      # 集計セットの初期化
      # グループキーと集計項目がマージされたmapを作る
      # generating a map to merge group key and summary columns
      # res_map =  %{ grp_key1 => val1, grp_key2 => val2, ..., sum_col1 => 0, sum_col2 => 0...}
   
      res_map = Map.put_new(acc_rec, map_key, 
                  map_key
                  |> Map.merge( Enum.reduce( sum_cols, %{}, # 集計項目ごとに0をセットしてマージ
                                             &Map.put_new(&2, &1 ,0)))
                  ) 
                |> Map.get(map_key) #一旦 全体mapに追加して取り出す
   
      #   acc_rec = %{key_map => %{ year: "2015", code: "2001", sales: 3000 },{key_map => %{} }
      #   res_map = %{ year: "2015", code: "2001", sales: 3000 }
   
      Map.put_new( acc_rec, map_key, res_map )
      |> Map.update( map_key, res_map, fn inner_map -> intersect(inner_map, data_rec, sum_cols) end)
      # inner_mapとdata_recの集計項目をマッチングして加算
      #
    end)
    |> Map.values
  end 

  def test_data() do
    [
      %{"year": 2015,"name": "川端　慎吾","team": "(ヤ)","dasuu": 581,"hit": 195,"homerun": 8,"daten": 57,"steal": 4},
      %{"year": 2015,"name": "山田　哲人","team": "(ヤ)","dasuu": 557,"hit": 183,"homerun": 38,"daten": 100,"steal": 34},
      %{"year": 2015,"name": "筒香　嘉智","team": "(デ)","dasuu": 496,"hit": 157,"homerun": 24,"daten": 93,"steal": 0},
      %{"year": 2015,"name": "ルナ","team": "(中)","dasuu": 496,"hit": 145,"homerun": 8,"daten": 60,"steal": 11},
      %{"year": 2015,"name": "ロペス","team": "(デ)","dasuu": 516,"hit": 150,"homerun": 25,"daten": 73,"steal": 1},
      %{"year": 2015,"name": "平田　良介","team": "(中)","dasuu": 491,"hit": 139,"homerun": 13,"daten": 53,"steal": 11},
      %{"year": 2015,"name": "鳥谷　敬","team": "(神)","dasuu": 551,"hit": 155,"homerun": 6,"daten": 42,"steal": 9},
      %{"year": 2015,"name": "福留　孝介","team": "(神)","dasuu": 495,"hit": 139,"homerun": 20,"daten": 76,"steal": 1},
      %{"year": 2015,"name": "マートン","team": "(神)","dasuu": 544,"hit": 150,"homerun": 9,"daten": 59,"steal": 0},
      %{"year": 2015,"name": "梶谷　隆幸","team": "(デ)","dasuu": 520,"hit": 143,"homerun": 13,"daten": 66,"steal": 28},
      %{"year": 2015,"name": "新井　貴浩","team": "(広)","dasuu": 426,"hit": 117,"homerun": 7,"daten": 57,"steal": 3},
      %{"year": 2015,"name": "田中　広輔","team": "(広)","dasuu": 543,"hit": 149,"homerun": 8,"daten": 45,"steal": 6},
      %{"year": 2015,"name": "ゴメス","team": "(神)","dasuu": 520,"hit": 141,"homerun": 17,"daten": 72,"steal": 0},
      %{"year": 2015,"name": "エルナンデス","team": "(中)","dasuu": 498,"hit": 135,"homerun": 11,"daten": 58,"steal": 5},
      %{"year": 2015,"name": "雄平","team": "(ヤ)","dasuu": 551,"hit": 149,"homerun": 8,"daten": 60,"steal": 7},
      %{"year": 2015,"name": "坂本　勇人","team": "(巨)","dasuu": 479,"hit": 129,"homerun": 12,"daten": 68,"steal": 10},
      %{"year": 2015,"name": "畠山　和洋","team": "(ヤ)","dasuu": 512,"hit": 137,"homerun": 26,"daten": 105,"steal": 0},
      %{"year": 2015,"name": "大島　洋平","team": "(中)","dasuu": 565,"hit": 147,"homerun": 6,"daten": 27,"steal": 22},
      %{"year": 2015,"name": "バルディリス","team": "(デ)","dasuu": 465,"hit": 120,"homerun": 13,"daten": 56,"steal": 0},
      %{"year": 2015,"name": "菊池　涼介","team": "(広)","dasuu": 562,"hit": 143,"homerun": 8,"daten": 32,"steal": 19},
      %{"year": 2015,"name": "上本　博紀","team": "(神)","dasuu": 375,"hit": 95,"homerun": 4,"daten": 31,"steal": 19},
      %{"year": 2015,"name": "長野　久義","team": "(巨)","dasuu": 434,"hit": 109,"homerun": 15,"daten": 52,"steal": 3}
  ]
  end

    
end
