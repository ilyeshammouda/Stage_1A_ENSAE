from math import log, sqrt, pi, exp

from scipy.stats import norm





## define the call options price function

def bs_call(S, K, T, r, q, sigma):
    d1=(log(S / K) + (r - q + sigma ** 2 / 2.) * T) / sigma * sqrt(T)
    d2=d1 - sigma * sqrt(T)

    return S * exp(-q * T) * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)


## define the put options price function

def bs_put(S, K, T, r, q, sigma):
    d1 = (log(S / K) + (r - q + sigma ** 2 / 2.) * T) / sigma * sqrt(T)
    d2 = d1 - sigma * sqrt(T)
    return K * exp(-r * T) * norm.cdf(-d2) - S * exp(-q * T) * norm.cdf(-d1)
def vega(S, K, T, r, q, sigma):
    d1 = (log(S / K) + (r - q + sigma ** 2 / 2.) * T) / sigma * sqrt(T)
    return (S * exp(-q * T)* sqrt(T) * (norm.pdf(d1)))

def implied_vol(S0, K, T, r, market_price, option_type, q, vol_old, tol=0.00001):
    """Compute the implied volatility of a European Option
        S0: initial stock price
        K:  strike price
        T:  maturity
        r:  risk-free rate
        market_price: market observed price
        option_type: 'c' for call; 'p' for put
        vol_old: intial guess
        q: continuously compounded dividend yield
        tol: user choosen tolerance
    """
    max_iter = 200 #max number of iterations
    for k in range(max_iter):
        if option_type == 'c':
            bs_cal = bs_call(S0, K, T, r, q, vol_old)
            Cprime = vega(S0, K, T, r, q, vol_old)*100
            C = bs_cal - market_price
            vol_new = vol_old - C/Cprime
            bs_new = bs_call(S0, K, T, r, q, vol_new)
            if (abs(vol_old - vol_new) < tol or abs(bs_new - market_price) < tol):
                break
            vol_old = vol_new
        else:
            bs_putt = bs_put(S0, K, T, r, q, vol_old)
            Cprime = vega(S0, K, T, r, vol_old) * 100
            C = bs_putt - market_price
            vol_new = vol_old - C / Cprime
            bs_new = bs_call(S0, K, T, r, q, vol_new)
            if (abs(vol_old - vol_new) < tol or abs(bs_new - market_price) < tol):
                break

        implied_vol = vol_old

    return implied_vol
