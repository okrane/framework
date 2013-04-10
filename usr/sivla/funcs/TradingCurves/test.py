import QRLib
import matplotlib.pyplot as plt
from usr.dev.sivla.DBTools.SE import * 





volume = get_se ('volume-curve', 110, 4, '20100504').value['Usual day'][8:]
volatility = get_se('volatility-curve', 110, 4, '20100504').value['USUAL DAY'][3:]
market_impact = get_se('market-impact', 110, 4, '20100504').value['Overall']
gamma = market_impact[0]
kappa = market_impact[1]

    
tr_c = QRLib.ISTradingCurves(102, 500000, 1, 5e-9, 1e-8, kappa, gamma, volume, volatility,5e6)

plt.plot(range(len(tr_c[0])),  tr_c[0], color='red')
plt.plot(range(len(tr_c[1])),  tr_c[1], color='green')
plt.plot(range(len(tr_c[2])),  tr_c[2], color='green')
plt.show()


