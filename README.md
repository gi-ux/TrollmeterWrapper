# TrollmeterWrapper

Notebook example contains a small example of the usage.
There are two kinds of usage:
- Collect mode enable (DEFAULT), where you only specify the username, the script will collect data and process trollscore:
  
  ```Trollmeter.calculate_troll_score("user")```
- Collect mode disabled, where you pass data you already have:

    ```Trollmetercalculate_troll_score("user",False, df_tw_a, df_m_a, df_rp_a, df_RT_a, df_RT_m, df_rp_m, df_m_m)```

Trollscore is number between 0 and 1, threshold calculated 0.38 

_Split.py_ contains the method to split DataFrame in SubDataFrames for the algorithm 