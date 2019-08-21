import math


class CurveFunction(object):
    def __init__(self, target_volume, order_count, last_row_volume):
        self.V = target_volume
        self.N = order_count
        self.M = last_row_volume
        self.curve_constant = None

    def calc(self, x):
        return self._curve_f(x, self.curve_constant, self.N, self.M)

    def configure(self):
        def newtons_method(v, n, m, x0, err):
            def calc_delta(x):
                return abs(0 - self._f(x, v, n, m))

            delta = calc_delta(x0)
            while delta > err:
                x0 = x0 - self._f(x0, v, n, m)/self._df(x0, n, m)
                delta = calc_delta(x0)
            return x0

        initial_guess = 0.2
        error_margin = 1e-5
        self.curve_constant = newtons_method(self.V, self.N, self.M, initial_guess, error_margin)

    
    def _curve_f(self, x, a, n, m):
        b = m - 1 + math.exp(a * n)
        return -math.exp(a * x) + b + 1

    def _f(self, x, v, n, m):
        exn = math.exp(n * x)
        return ((1 - exn) / (x * n)) + exn + m - (v / n)

    def _df(self, x, n, m):
        exn = math.exp(n * x)
        return -(exn / x) + (n * exn) - ((1 - exn) / (n * math.pow(x, 2)))


class LinearFunction(object):
    def __init__(self, target_volume, order_count):
        self.target_volume = target_volume
        self.order_count = order_count

    def calc(self, index):
        def _curve_f(x, v, n):
            v2_n3 = (2 * v) / (3 * n)
            return (2 * v2_n3) - ((v2_n3 / n) * x)
        return _curve_f(index, self.target_volume, self.order_count)


class VolumeCoordinator(object):
    def __init__(self, target_volume_usd, target_num_orders):
        last_order_size = 70
        min_order_size = 50
        order_threshhold_ratio = 0.6
        
        self.target_num_orders = target_num_orders
        self.target_volume_curve = CurveFunction(target_volume_usd, target_num_orders, last_order_size)
        self.minimum_volume_curve = CurveFunction(target_volume_usd * order_threshhold_ratio, 
                                                  target_num_orders, min_order_size)

        try: 
            self.target_volume_curve.configure()
            self.minimum_volume_curve.configure()
        except Exception:
            # If the proper curve cannot be calculated with the given inputs
            # fallback to a linear curve
            self.target_volume_curve = LinearFunction(target_volume_usd, target_num_orders)
            self.minimum_volume_curve = LinearFunction(target_volume_usd * order_threshhold_ratio, target_num_orders)

    def target_usd_volume_at(self, index, price=None):
        return self.target_volume_curve.calc(index + 1)

    def min_usd_volume_at(self, index, price=None):
        return self.minimum_volume_curve.calc(index + 1)
