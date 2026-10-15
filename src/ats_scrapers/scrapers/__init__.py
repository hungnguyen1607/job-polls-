"""ATS scrapers — one class per platform.

Each scraper is a thin, dependency-light fetch+parse layer that returns
`Job` instances. Discovery, enrichment, deduplication, and publishing are
kept outside the public scraper API so each scraper stays usable on its own.

>>> from ats_scrapers.scrapers import GreenhouseScraper
>>> jobs = GreenhouseScraper("anthropic").fetch()
"""

from ats_scrapers.scrapers.adp import ADPWorkforceNowScraper
from ats_scrapers.scrapers.amazon import AmazonScraper
from ats_scrapers.scrapers.apple import AppleScraper
from ats_scrapers.scrapers.ashby import AshbyScraper
from ats_scrapers.scrapers.avature import AvatureScraper
from ats_scrapers.scrapers.bamboohr import BambooHRScraper
from ats_scrapers.scrapers.base import BaseScraper, ScraperRegistry, get_scraper
from ats_scrapers.scrapers.breezy import BreezyScraper
from ats_scrapers.scrapers.builtin import BuiltInScraper
from ats_scrapers.scrapers.bytedance import BytedanceScraper
from ats_scrapers.scrapers.cornerstone import CornerstoneScraper
from ats_scrapers.scrapers.dayforce import DayforceScraper
from ats_scrapers.scrapers.eightfold import EightfoldScraper
from ats_scrapers.scrapers.gem import GemScraper
from ats_scrapers.scrapers.google import GoogleScraper
from ats_scrapers.scrapers.greenhouse import GreenhouseScraper
from ats_scrapers.scrapers.icims import iCIMSScraper
from ats_scrapers.scrapers.jazzhr import JazzHRScraper
from ats_scrapers.scrapers.jobbankca import JobBankCAScraper
from ats_scrapers.scrapers.jobvite import JobviteScraper
from ats_scrapers.scrapers.lever import LeverScraper
from ats_scrapers.scrapers.mercor import MercorScraper
from ats_scrapers.scrapers.meta import MetaScraper
from ats_scrapers.scrapers.oracle import OracleScraper
from ats_scrapers.scrapers.pageup import PageUpScraper
from ats_scrapers.scrapers.paycom import PaycomScraper
from ats_scrapers.scrapers.paylocity import PaylocityScraper
from ats_scrapers.scrapers.phenom import PhenomScraper
from ats_scrapers.scrapers.pinpoint import PinpointScraper
from ats_scrapers.scrapers.recruitee import RecruiteeScraper
from ats_scrapers.scrapers.recruiterbox import RecruiterboxScraper
from ats_scrapers.scrapers.remoteok import RemoteOKScraper
from ats_scrapers.scrapers.rippling import RipplingScraper
from ats_scrapers.scrapers.smartrecruiters import SmartRecruitersScraper
from ats_scrapers.scrapers.successfactors import SuccessFactorsScraper
from ats_scrapers.scrapers.taleo import TaleoScraper
from ats_scrapers.scrapers.tesla import TeslaScraper
from ats_scrapers.scrapers.tiktok import TikTokScraper
from ats_scrapers.scrapers.uber import UberScraper
from ats_scrapers.scrapers.ukg import UKGProScraper
from ats_scrapers.scrapers.usajobs import USAJobsScraper
from ats_scrapers.scrapers.wellfound import WellfoundScraper
from ats_scrapers.scrapers.weworkremotely import WeWorkRemotelyScraper
from ats_scrapers.scrapers.workable import WorkableScraper
from ats_scrapers.scrapers.workday import WorkdayScraper
from ats_scrapers.scrapers.ycombinator import YCombinatorScraper

__all__ = [
    "ADPWorkforceNowScraper",
    "AmazonScraper",
    "AppleScraper",
    "AshbyScraper",
    "AvatureScraper",
    "BambooHRScraper",
    "BaseScraper",
    "BreezyScraper",
    "BuiltInScraper",
    "BytedanceScraper",
    "CornerstoneScraper",
    "DayforceScraper",
    "EightfoldScraper",
    "GemScraper",
    "GoogleScraper",
    "GreenhouseScraper",
    "JazzHRScraper",
    "JobBankCAScraper",
    "JobviteScraper",
    "LeverScraper",
    "MercorScraper",
    "MetaScraper",
    "OracleScraper",
    "PageUpScraper",
    "PaycomScraper",
    "PaylocityScraper",
    "PhenomScraper",
    "PinpointScraper",
    "RecruiteeScraper",
    "RecruiterboxScraper",
    "RemoteOKScraper",
    "RipplingScraper",
    "ScraperRegistry",
    "SmartRecruitersScraper",
    "SuccessFactorsScraper",
    "TaleoScraper",
    "TeslaScraper",
    "TikTokScraper",
    "UKGProScraper",
    "USAJobsScraper",
    "UberScraper",
    "WeWorkRemotelyScraper",
    "WellfoundScraper",
    "WorkableScraper",
    "WorkdayScraper",
    "YCombinatorScraper",
    "get_scraper",
    "iCIMSScraper",
]
