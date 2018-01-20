# Aggex

## Aggregating from a Map. Grouping by a key list. 

```elixir
# iex -S mix
iex> Aggex.group_by( Aggex.test_data, ["year","team"], ["homerun", "hit"] )

[%{hit: 570, homerun: 75, team: "(デ)"},
  %{hit: 664, homerun: 80, team: "(ヤ)"},
  %{hit: 566, homerun: 38, team: "(中)"},
  %{hit: 238, homerun: 27, team: "(巨)"},
  %{hit: 409, homerun: 23, team: "(広)"},
  %{hit: 680, homerun: 56, team: "(神)"}]}
```
  

  