from math import log, sqrt, pi, exp

from scipy.stats import norm


## define two functions, d1 and d2 in Black-Scholes model

def d1(S, K, T, r, q, sigma):
    return (log(S / K) + (r - q + sigma ** 2 / 2.) * T) / sigma * sqrt(T)


def d2(S, K, T, r, q, sigma):
    return d1(S, K, T, r, q, sigma) - sigma * sqrt(T)


## define the call options price function

def bs_call(S, K, T, r, q, sigma):
    return S * exp(-q * T) * norm.cdf(d1(S, K, T, r, q, sigma)) - K * exp(-r * T) * norm.cdf(d2(S, K, T, r, q, sigma))


## define the put options price function

def bs_put(S, K, T, r, q, sigma):
    return K * exp(-r * T) - S * exp(-q * T) + bs_call(S, K, T, r, q, sigma)


# Implied Volatility using bisection


def implied_vol(option_type, option_price, S, K, r, T, q):
    # apply bisection method to get the implied volatility by solving the BSM function

    precision = 0.00001

    upper_vol = 50.0

    max_vol = 50.0

    min_vol = 0.0001

    lower_vol = 0.0001

    iteration = 0

    while 1:

        iteration += 1

        mid_vol = (upper_vol + lower_vol) / 2.0

        if option_type == 'c':

            price = bs_call(S, K, T, r, q, mid_vol)

            lower_price = bs_call(S, K, T, r, q, lower_vol)

            if (lower_price - option_price) * (price - option_price) > 0:

                lower_vol = mid_vol

            else:

                upper_vol = mid_vol

            if abs(price - option_price) < precision: break

            if mid_vol > max_vol - 5:
                mid_vol = 0.0001

                break



        elif option_type == 'p':

            price = bs_put(S, K, T, r, q, mid_vol)

            upper_price = bs_put(S, K, T, r, q, upper_vol)

            if (upper_price - option_price) * (price - option_price) > 0:

                upper_vol = mid_vol

            else:

                lower_vol = mid_vol

            if abs(price - option_price) < precision: break

            if iteration > 100: break

    return mid_vol