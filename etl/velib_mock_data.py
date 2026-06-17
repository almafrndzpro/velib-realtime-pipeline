"""
Realistic simulated data for the ETL fallback.
Used only if the Paris Open Data API is unavailable.
"""

import random
from datetime import datetime, timezone

# 30 representative stations covering Paris and its immediate suburbs
_STATIONS = [
    (1001, "Châtelet - Place du Châtelet",       48.8559, 2.3471, 30),
    (1002, "Hôtel de Ville - Lobau",             48.8566, 2.3508, 25),
    (2001, "Opéra - Boulevard des Capucines",    48.8700, 2.3318, 25),
    (2002, "Madeleine - Tronchet",               48.8702, 2.3247, 20),
    (3001, "Bastille - Place de la Bastille",    48.8534, 2.3692, 35),
    (3002, "Nation - Place de la Nation",        48.8481, 2.3960, 30),
    (4001, "Montparnasse - Edgar Quinet",        48.8428, 2.3247, 28),
    (4002, "Denfert-Rochereau - Mouton Duvernet",48.8337, 2.3312, 22),
    (5001, "Saint-Michel - Place Saint-Michel",  48.8530, 2.3463, 20),
    (5002, "Odéon - Carrefour de l'Odéon",      48.8516, 2.3393, 18),
    (6001, "Invalides - Place des Invalides",    48.8560, 2.3123, 30),
    (6002, "Champ de Mars - Tour Eiffel",        48.8575, 2.2945, 40),
    (7001, "Palais-Royal - Place du Palais-Royal",48.8638, 2.3367, 25),
    (7002, "Louvre - Rue de Rivoli",             48.8606, 2.3430, 30),
    (8001, "Gare du Nord - Rue de Dunkerque",    48.8806, 2.3548, 35),
    (8002, "Gare de Lyon - Place Louis Armand",  48.8443, 2.3740, 40),
    (9001, "République - Place de la République",48.8674, 2.3637, 38),
    (9002, "Oberkampf - Parmentier",             48.8643, 2.3765, 22),
    (10001,"Pigalle - Place Pigalle",            48.8826, 2.3342, 25),
    (10002,"Abbesses - Place des Abbesses",      48.8842, 2.3385, 20),
    (11001,"Marais - Rue de Bretagne",           48.8624, 2.3628, 20),
    (11002,"Beaubourg - Centre Pompidou",        48.8606, 2.3522, 22),
    (12001,"Belleville - Place de Belleville",   48.8723, 2.3826, 25),
    (12002,"Père Lachaise - Boulevard Voltaire", 48.8611, 2.3877, 28),
    (13001,"Vincennes - Château de Vincennes",   48.8434, 2.4390, 30),
    (13002,"Boulogne - Pont de Saint-Cloud",     48.8350, 2.2172, 25),
    (14001,"Saint-Denis - Hôtel de Ville",       48.9362, 2.3588, 22),
    (14002,"Montreuil - Croix de Chavaux",       48.8626, 2.4407, 20),
    (15001,"Levallois - Louise Michel",          48.8966, 2.2866, 18),
    (15002,"Neuilly - Porte Maillot",            48.8786, 2.2834, 28),
]


def generate_mock_data(batch_id: str) -> list[dict]:
    """Génère des données réalistes simulées pour toutes les stations."""
    records = []
    now_ts = int(datetime.now(timezone.utc).timestamp())

    for num, name, lat, lon, cap in _STATIONS:
        # Distribution réaliste : taux de remplissage entre 10% et 90%
        fill_pct = random.betavariate(2, 2)          # concentré autour de 50%
        bikes    = max(0, min(cap, round(cap * fill_pct)))
        stands   = cap - bikes

        records.append({
            "station_number":   num,
            "station_name":     name,
            "address":          "Paris",
            "latitude":         lat + random.uniform(-0.0001, 0.0001),
            "longitude":        lon + random.uniform(-0.0001, 0.0001),
            "banking":          True,
            "bonus":            False,
            "bike_stands":      cap,
            "available_bikes":  bikes,
            "available_stands": stands,
            "status":           "OPEN",
            "last_update":      now_ts,
            "batch_id":         batch_id,
        })

    return records
