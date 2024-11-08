# datascience

This repo contains useful resources for analyzing sports and sports betting data.
It contains the backtesting framework which can be used to quickly test different betting strategy by re-playing past stream of bets.

## +EV AI Challenge

If you reached this repo because you are interested on the +EV AI challenge, feel free to have a look at `notebooks/pos_ev_challenge.ipynb` for the details of the challenge.

Feel free to mail your notebook with the AI algorithm and the `/data/output` folder to Vincenzo Ampolo <vincenzo@oddsjam.com> for evaluation.

Whoever can beat OddsJam AI picking EV bets will grant a prize of $3000 USD.

## How to use

```
poetry install
```

In vscode you can open the `notebooks/pos_ev_challenge.ipynb` file to see how to call the backtesting function.


## What can I use this framework for?

This framework allow you to write your own betting strategy and select the bets to pick everytime there is a change in a market for a match.
This allows to test different strategies to pick the bets. The bets picked are graded and saved in the `data/output` folder so other analysis can be done in terms of ROI and distribution of the bets in terms of sportsbooks and markets.
