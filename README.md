# datascience

This repo contains useful resources for analyzing sports and sports betting data.
It contains the backtesting framework which can be used to quickly test different betting strategy by re-playing past stream of bets.

## How to use

```
poetry install
```

In vscode you can open the `notebooks/example.ipynb` file to see how to call the backtesting function.


## What can I use this framework for?

This framework allow you to write your own betting strategy and select the bets to pick everytime there is a change in a market for a match.
This allows to test different strategies to pick the bets. The bets picked are graded and saved in the `data/output` folder so other analysis can be done in terms of ROI and distribution of the bets in terms of sportsbooks and markets.


