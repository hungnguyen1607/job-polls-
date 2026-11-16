"""Major US/Canada metro areas and a matcher for free-form job location strings.

Shared by the outreach page (scripts/view_local.py) and the office-location builder
(scripts/office_locations.py).
"""
from __future__ import annotations

import re

# Major US/Canada metros. Suburbs fold into their metro (Redmond -> Seattle, Markham -> Toronto)
# so "which companies have a Toronto office" is one filter. Ambiguous names (London, Victoria,
# Kingston, Hamilton, Cambridge, Richmond, Washington) need their province/state to match.
METROS: list[tuple[str, str, str]] = [
    ("Toronto", "CA", r"toronto|mississauga|markham|richmond hill|vaughan|oakville|brampton|"
                      r"north york|scarborough|etobicoke|burlington, on"),
    ("Montreal", "CA", r"montr[eé]al|dorval|laval|saint-laurent|longueuil|pointe-claire|mirabel"),
    ("Vancouver", "CA", r"vancouver(?!, wa)|burnaby|richmond, (?:bc|british)|surrey, (?:bc|british)"),
    ("Ottawa", "CA", r"ottawa|kanata|gatineau"),
    ("Waterloo", "CA", r"waterloo|kitchener|cambridge, (?:on|ontario)"),
    ("Calgary", "CA", r"calgary"),
    ("Edmonton", "CA", r"edmonton"),
    ("Winnipeg", "CA", r"winnipeg"),
    ("Halifax", "CA", r"halifax"),
    ("Quebec City", "CA", r"qu[eé]bec city|ville de qu[eé]bec|qu[eé]bec, qc"),
    ("Victoria", "CA", r"victoria, (?:bc|british)"),
    ("Hamilton", "CA", r"hamilton, (?:on|ontario)"),
    ("London ON", "CA", r"london, (?:on|ontario)"),
    ("Kingston ON", "CA", r"kingston, (?:on|ontario)"),
    ("Saskatoon", "CA", r"saskatoon"),
    ("Regina", "CA", r"regina"),
    ("SF Bay Area", "US", r"san francisco|\bsf\b|san jose|palo alto|santa clara|sunnyvale|"
                          r"mountain view|menlo park|cupertino|milpitas|foster city|redwood city|"
                          r"san mateo|oakland|fremont|berkeley|south san francisco|san bruno|"
                          r"emeryville|burlingame|bay area"),
    ("New York", "US", r"new york|\bnyc\b|brooklyn|manhattan|jersey city|hoboken"),
    ("Seattle", "US", r"seattle|redmond|bellevue|kirkland"),
    ("Los Angeles", "US", r"los angeles|santa monica|el segundo|culver city|irvine|pasadena|"
                          r"playa vista|burbank|long beach|torrance"),
    ("Boston", "US", r"boston|cambridge, (?:ma|massachusetts)|waltham|burlington, ma|somerville"),
    ("Washington DC", "US", r"washington,? d\.?c|mclean|arlington, va|reston|herndon|"
                            r"chantilly|bethesda|tysons"),
    ("Austin", "US", r"austin"),
    ("Chicago", "US", r"chicago"),
    ("Dallas", "US", r"dallas|plano|irving|richardson|fort worth|frisco"),
    ("Houston", "US", r"houston"),
    ("Atlanta", "US", r"atlanta|alpharetta"),
    ("Denver", "US", r"denver|boulder"),
    ("San Diego", "US", r"san diego"),
    ("Pittsburgh", "US", r"pittsburgh"),
    ("Philadelphia", "US", r"philadelphia"),
    ("Miami", "US", r"miami|fort lauderdale"),
    ("Phoenix", "US", r"phoenix|tempe|chandler|scottsdale"),
    ("Portland", "US", r"portland|hillsboro|beaverton"),
    ("Minneapolis", "US", r"minneapolis|st\.? paul"),
    ("Detroit", "US", r"detroit|dearborn|ann arbor"),
    ("Salt Lake City", "US", r"salt lake|lehi|provo"),
    ("Raleigh-Durham", "US", r"raleigh|durham(?!, (?:on|ontario))|\bcary\b|research triangle"),
    ("Charlotte", "US", r"charlotte"),
    ("Nashville", "US", r"nashville"),
    ("Columbus", "US", r"columbus, oh"),
]
METRO_RES = [(name, country, re.compile(pat, re.I)) for name, country, pat in METROS]

def metros_in(location: str) -> list[str]:
    return [name for name, _, rx in METRO_RES if rx.search(location or "")]
