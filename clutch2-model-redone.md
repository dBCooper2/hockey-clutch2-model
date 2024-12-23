---
title: Defining Clutch - Hype Moments in the NHL
description: Creating a Win Probability Model for hockey to classify shots as clutch.
date: 2024-12-20
tags: ["hockey","data-analysis","python"]
published: true
---

###### By:: Trevor Rowland ([dBCooper2](<https://github.com/dBCooper2>)), Abhishek Menothu ([sh4ken](<https://github.com/sh4ken>)) and Johnathen Wigfall ([jwigfall4627](<https://github.com/Jwigfall4627>))

## Abstract

### Problem Statement

With the time waning, the score is tied. Fans lean forward, sticks clash, and every pass is loaded
with consequence. In these last moments, players need abilities beyond skill to draw from: stamina, grit,
and the illusory ‘clutch’ factor. As fans of hockey, how is clutch defined? From that definition, how can
we rank players and teams to know which team is the most clutch, and therefore the most exciting? With
games competing in the same time slot on TV, and rising prices for tickets, how can a fan be sure that
their chosen experience will be the high-octane event that hockey is known to be? This notebook aims to
create a working definition for Clutch in the NHL, and classify teams and players who model this
definition.

### Method Overview

By using Michael MacKelvie's video, "[The Clutch GOAT...(it's not who you think).](<https://www.youtube.com/watch?v=qjjW1l9KjXQ>)", as a springboard to start figuring out a definition of clutch in the NHL. In the video, Mackelvie uses Mike Beuoy's Clutch<sup>2</sup> model, which uses a Win Probability Added Model to find which shots in the NBA moved the win probability of a team more than an average amount, and those shots are called "Clutch<sup>2</sup>". Taking this to the NHL is fairly straightforward, we just need to create a Win Probability Model for hockey games.

### Key Findings

With our model, we were able to rank players from the 2023-2024 and 2024 season by their "clutch-ness". We found that the top 5 teams by clutch<sup>2</sup> goal count were: the Edmonton Oilers, Florida Panthers, Dallas Stars, Carolina Hurricanes, and the Colorado Avalanche. 

The top 5 players by clutch<sup>2</sup> goal count were: Zach Hyman(EDM), Sam Reinhart(FLA), Auston Matthews(TOR), Nathan MacKinnon(COL), and Artemi Panarin(NYR). These line up with the players who were most impactful in the 2023 season: Zach Hyman's incredible run in the playoff, Auston Matthews with his 70-goal season, Sam Reinhart with his Stanley Cup-Winning performance, and MacKinnon and Panarin with their deep playoff runs. It was interesting to see a players on the Stars or the Hurricanes showing up in the top 5, but this could be a a more even distribution of goals across players, versus a player like Hyman who scored a significant portion of the Oiler's goals.

### Main Impact

## Introduction

### What is known

### Our world understanding

## Literature Reviews

### What is unknown?

### What is the gap we want to fill

## Methodology

### How we should fill the gap?

### What did you do?

## Results

### What findings did you get?

## Discussion

### How do the findings bridge the gap?

## Conclusion

- Teams who share their "clutch-ness" dont appear as high up because this is a better ranking for players and not teams.

### What does this mean for us going forward?

## References