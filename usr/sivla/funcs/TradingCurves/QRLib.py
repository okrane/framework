import sys
sys.path.append('C:/Libs/volume_curves/src/volumeCurveWrapper')
import TradingCurves

dblArray = TradingCurves.doubleArray

def VwapTradingCurves(n, order_size, a_up, a_down, lam, kappa, gamma, volume_curve, volatility_curve, market_volume ):    
    
    arg_volume_curve = dblArray(n)
    arg_volatility_curve = dblArray(n)
    for i in range(n):
        arg_volume_curve[i] = volume_curve[i]
        arg_volatility_curve[i] = volatility_curve[i]
    
    rez = TradingCurves.VwapTrendTrCurves_computeTradingCurves(n, order_size, a_up, a_down, lam, kappa, gamma, arg_volume_curve, arg_volatility_curve, market_volume)
    tr_c = []    
    w_up = []
    w_down = []
    
    for i in range(n+1):
        tr_c.append(TradingCurves.get_elem(rez, 0, i))
        w_up.append(TradingCurves.get_elem(rez, 1, i))
        w_down.append(TradingCurves.get_elem(rez, 2, i))
    
    return [tr_c, w_up, w_down]  

def ISTradingCurves(n, order_size, a_up, a_down, lam, kappa, gamma, volume_curve, volatility_curve, market_volume ):    
    
    arg_volume_curve = dblArray(n)
    arg_volatility_curve = dblArray(n)
    for i in range(n):
        arg_volume_curve[i] = volume_curve[i]
        arg_volatility_curve[i] = volatility_curve[i]
    
    rez = TradingCurves.ISPortofolioTrCurves_computeTradingCurves(n, order_size, a_up, a_down, lam, kappa, gamma, arg_volume_curve, arg_volatility_curve, market_volume)
    tr_c = []    
    w_up = []
    w_down = []
    
    for i in range(n+1):
        tr_c.append(TradingCurves.get_elem(rez, 0, i))
        w_up.append(TradingCurves.get_elem(rez, 1, i))
        w_down.append(TradingCurves.get_elem(rez, 2, i))
    
    return [tr_c, w_up, w_down]