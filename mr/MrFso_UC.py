from cr_monitor.mr.Mr_DTF import MrDTF

class FsoUC(MrDTF):
    """Fso means future and spot in okex, while UC means usdt-swap and usd-future. No matter what is master.

    Args:
        MrDTF (_type_): DTF means master is usd-future and slave is usdt-swap. Only in okex
    """
    def __init__(self, amount_u: int, amount_c: int, amount_fund: float, price_u: float, price_c: float, now_price: float, suffix: str, coin="BTC", is_long=True):
        super().__init__(amount_u, amount_c, amount_fund, price_u, price_c, now_price, suffix, coin, is_long)