# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 18:06:35 2013

@author: svlasceanu
"""
import numpy as np
import matplotlib.pyplot as plt
from lib.plots.color_schemes import kc_main_colors
# Vwap

SlippageVWAP_K = np.array([-1.87,-2.47,-1.67,-1.70,-2.84,-2.70,-2.58,-1.51,-2.07,-2.53,-0.55])
StdDevpVWAP_K = np.array([	14.31,15.29,13.80	, 16.99,20.26,21.65,14.80,17.27,12.01,28.04,12.61])

SlippageVWAP_C = np.array([-1.51,-2.23,-1.65,-2.07,-1.84])
StdDevpVWAP_C =np.array([1.12,	1.83,	2.26,	2.23,	1.86])
Nbocc = np.array([282.91,310.47,286.92,264.40,286.54])

fig, ax = plt.subplots(1, 1)
ax.plot(range(1, len(SlippageVWAP_K)+1), np.zeros(len(SlippageVWAP_K)), '--', color = 'k', linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), SlippageVWAP_K, 'x', color = kc_main_colors()["dark_blue"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), SlippageVWAP_K, color = kc_main_colors()["dark_blue"],  linewidth = 2  )
ax.plot(range(1, len(SlippageVWAP_K)+1), np.ones(len(SlippageVWAP_K)) * np.mean(SlippageVWAP_K), '--', color = kc_main_colors()["dark_blue"], linewidth = 2 )
#ax.plot(range(1, len(StdDevpVWAP_K)+1), SlippageVWAP_K + StdDevpVWAP_K, "--", color = kc_main_colors()["dark_blue"], linewidth = 2 )
#ax.plot(range(1, len(StdDevpVWAP_K)+1), SlippageVWAP_K - StdDevpVWAP_K, "--", color = kc_main_colors()["dark_blue"], linewidth = 2 )

ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C, 'x', color = kc_main_colors()["orange"],linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C, color = kc_main_colors()["orange"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), np.ones(len(SlippageVWAP_K)) * np.mean(SlippageVWAP_C), '--', color = kc_main_colors()["orange"], linewidth = 2 )
#ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C + StdDevpVWAP_C * np.sqrt(Nbocc), "--", color = kc_main_colors()["orange"], linewidth = 2 )
#ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C - StdDevpVWAP_C * np.sqrt(Nbocc), "--", color = kc_main_colors()["orange"], linewidth = 2 )
ax.set_title("Comparison between performance (VWAP)")
ax.set_ylabel("Slippage VWAP(bp)")
ax.set_xlabel("Month")
ax.grid(True)
plt.show()


# Volume & Dyn Vol

SlippageVWAP_K = np.array([-1.47,	-0.97,-1.10,-1.49,-0.77,-0.71,-0.86,-0.92,-0.81,-0.44,-1.02,-0.82])
StdDevpVWAP_K = np.array([	14.31,15.29,13.80	, 16.99,20.26,21.65,14.80,17.27,12.01,28.04,12.61])

SlippageVolume_K = np.array([-2.02,-1.49,-2.66,-1.47,-2.67,-1.71,-0.44,-0.46,-1.62,-0.31,-2.80])

SlippageVWAP_C = np.array([-1.76,-1.74,-1.92,-2.03,-1.85])
StdDevpVWAP_C =np.array([1.12,	1.83,	2.26,	2.23,	1.86])
Nbocc = np.array([282.91,310.47,286.92,264.40,286.54])

fig, ax = plt.subplots(1, 1)
ax.plot(range(1, len(SlippageVWAP_K)+1), np.zeros(len(SlippageVWAP_K)), '--', color = 'k', linewidth = 2 )

ax.plot(range(1, len(SlippageVWAP_K)+1), SlippageVWAP_K, 'x', color = kc_main_colors()["dark_blue"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), SlippageVWAP_K, color = kc_main_colors()["dark_blue"],  linewidth = 2  )
ax.plot(range(1, len(SlippageVWAP_K)+1), np.ones(len(SlippageVWAP_K)) * np.mean(SlippageVWAP_K), '--', color = kc_main_colors()["dark_blue"], linewidth = 2 )
#ax.plot(range(1, len(StdDevpVWAP_K)+1), SlippageVWAP_K + StdDevpVWAP_K, "--", color = kc_main_colors()["dark_blue"], linewidth = 2 )
#ax.plot(range(1, len(StdDevpVWAP_K)+1), SlippageVWAP_K - StdDevpVWAP_K, "--", color = kc_main_colors()["dark_blue"], linewidth = 2 )
ax.plot(range(1, len(SlippageVolume_K)+1), SlippageVolume_K, 'x', color = kc_main_colors()["light_blue"], linewidth = 2 )
ax.plot(range(1, len(SlippageVolume_K)+1), SlippageVolume_K, color = kc_main_colors()["light_blue"],  linewidth = 2  )
ax.plot(range(1, len(SlippageVolume_K)+1), np.ones(len(SlippageVolume_K)) * np.mean(SlippageVolume_K), '--', color = kc_main_colors()["light_blue"], linewidth = 2 )



ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C, 'x', color = kc_main_colors()["orange"],linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C, color = kc_main_colors()["orange"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), np.ones(len(SlippageVWAP_K)) * np.mean(SlippageVWAP_C), '--', color = kc_main_colors()["orange"], linewidth = 2 )
#ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C + StdDevpVWAP_C * np.sqrt(Nbocc), "--", color = kc_main_colors()["orange"], linewidth = 2 )
#ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C - StdDevpVWAP_C * np.sqrt(Nbocc), "--", color = kc_main_colors()["orange"], linewidth = 2 )
ax.set_title("Comparison between performance (Volume)")
ax.set_ylabel("Slippage VWAP(bp)")
ax.set_xlabel("Month")
ax.grid(True)
plt.show()



# IS

SlippageVWAP_K = np.array([-7.45, 0, 0,	5.74,	17.36, 1.93 ,-5.478, 14, -5.12, -32.71,6.53])
StdDevpVWAP_K = np.array([	14.31,15.29,13.80	, 16.99,20.26,21.65,14.80,17.27,12.01,28.04,12.61])

SlippageVWAP_C = np.array([2.46,3.23,1.14,-0.79,2.01])
StdDevpVWAP_C =np.array([1.12,	1.83,	2.26,	2.23,	1.86])
Nbocc = np.array([282.91,310.47,286.92,264.40,286.54])

fig, ax = plt.subplots(1, 1)

ax.plot(range(1, len(SlippageVWAP_K)+1), SlippageVWAP_K, 'x', color = kc_main_colors()["dark_blue"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), SlippageVWAP_K, color = kc_main_colors()["dark_blue"],  linewidth = 2  )
ax.plot(range(1, len(SlippageVWAP_K)+1), np.ones(len(SlippageVWAP_K)) * np.mean(SlippageVWAP_K), '--', color = kc_main_colors()["dark_blue"], linewidth = 2 )
#ax.plot(range(1, len(StdDevpVWAP_K)+1), SlippageVWAP_K + StdDevpVWAP_K, "--", color = kc_main_colors()["dark_blue"], linewidth = 2 )
#ax.plot(range(1, len(StdDevpVWAP_K)+1), SlippageVWAP_K - StdDevpVWAP_K, "--", color = kc_main_colors()["dark_blue"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C, 'x', color = kc_main_colors()["orange"],linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C, color = kc_main_colors()["orange"], linewidth = 2 )
ax.plot(range(1, len(SlippageVWAP_K)+1), np.ones(len(SlippageVWAP_K)) * np.mean(SlippageVWAP_C), '--', color = kc_main_colors()["orange"], linewidth = 2 )

#ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C + StdDevpVWAP_C * np.sqrt(Nbocc), "--", color = kc_main_colors()["orange"], linewidth = 2 )
#ax.plot(range(1, len(SlippageVWAP_C)+1), SlippageVWAP_C - StdDevpVWAP_C * np.sqrt(Nbocc), "--", color = kc_main_colors()["orange"], linewidth = 2 )
ax.set_title("Comparison between performance (Implementation Shortfall)")
ax.set_ylabel("Slippage Arrival Price(bp)")
ax.set_xlabel("Month")
ax.grid(True)
plt.show()



