#!/usr/bin/env python3
import os
import re
import logging
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Any

from pymongo import MongoClient

try:
    from motor.motor_asyncio import AsyncIOMotorDatabase
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False

from flashtext import KeywordProcessor

STAGING_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
STAGING_DB = os.environ.get("MONGO_DB", "jobminglr_staging")
PROD_URI = os.environ.get("PROD_URI", "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority")
PROD_DB = os.environ.get("PROD_DB", "jobminglr")
USE_STAGING = os.environ.get("USE_STAGING", "true").lower() == "true"
SKILLS_COLLECTION = "skills"

CASE_SENSITIVE = False
RETURN_SPANS = True
MAX_ALIAS_LEN = 200
MIN_TOKEN_LEN = 2

NOISY_CANONICAL = {
    "c", "r", "j", "d", "e", "f", "k", "m", "n", "p", "s", "t",
    "go", "it", "hr", "qa", "pm", "os",
    "bi", "do", "in", "on", "up", "no", "so", "we", "us", "my",
    "an", "as", "at", "be", "by", "he", "if", "is", "me", "of",
    "or", "to",
    "abacus", "abbot", "abyss", "access", "accordion", "accuracy", "acme", "acorn",
    "across", "act", "action", "ada", "adjust", "advance", "aeries", "affinity",
    "agility", "aim", "ajax", "aladdin", "alexandria", "alfred", "alfresco", "align",
    "allegro", "alloy", "alma", "altair", "alterations", "amadeus", "amber",
    "amortization", "amplitude", "amundsen", "anchor", "ant", "ants", "ape", "apex",
    "apply", "approach", "apricot", "aquaculture", "arbor", "arbutus", "archer", "area",
    "arena", "argus", "aries", "arius", "armadillo", "arnold", "artemis", "artillery",
    "ascend", "aspire", "asset", "asterisk", "athena", "atoll", "audacity", "augustus",
    "autopsy", "avast", "avogadro", "axiom",
    "babylon", "backstage", "baidu", "banner", "barracuda", "bart", "bartender", "base",
    "bazaar", "beaker", "bellhop", "best", "bison", "blackboard", "blend", "blogger",
    "board", "bolero", "booker", "boulevard", "box", "brackets", "brainstorm", "brave",
    "breakpoints", "bridge", "bryce", "buffer", "bugzilla", "build", "bulldozer",
    "bullhorn", "bun", "burn", "buzz", "byte", "byword",
    "cacti", "caddy", "cairo", "calculator", "calypso", "campfire", "candy", "canoe",
    "canopy", "cantaloupe", "cantata", "canto", "canute", "capital", "capture",
    "catapult", "categorization", "certify", "chameleon", "change", "chapel", "checklist",
    "cheetah", "chew", "chime", "chimera", "chisel", "choice", "choreograph",
    "chronicle", "chuck", "chyron", "cinderella", "clang", "cleo", "clio", "clipper",
    "clockwork", "close", "clover", "cloverleaf", "coach", "cobalt", "cocoa", "cogent",
    "collaboration", "colleague", "colossus", "commander", "communication", "compass",
    "compound", "compress", "compressor", "concordance", "concur", "conductor",
    "confluent", "conga", "conquest", "consed", "consul", "consume", "context",
    "contour", "coot", "coordinator", "copper", "core", "cornerstone", "corridor",
    "cosmos", "costar", "counterpoint", "create", "credible", "crystal", "cube",
    "cucumber", "curate", "curl", "curry", "cursor", "cycles", "cygnet", "cyrillic",
    "dabble", "dante", "dapper", "darwin", "dash", "decipher", "deckhand",
    "delegation", "density", "deputy", "descartes", "design", "dialog", "diameter",
    "diamond", "dice", "dig", "diligent", "dimensions", "dips", "disco", "divvy",
    "doctrine", "dolphin", "doodle", "doris", "drafty", "dragonfly", "drake",
    "draping", "drift", "drive", "drools",
    "dylan", "eagle", "ease", "east", "echos", "edge", "edger", "egress", "eiffel",
    "elevate", "emacs", "embark", "emblem", "emerald", "empower", "emu", "encase",
    "encircle", "encompass", "encore", "engage", "enigma", "entrust", "envision",
    "envoy", "epic", "epicenter", "epicure", "epilog", "epsilon", "erupt", "erwin",
    "essential", "estimate", "everest", "evergreen", "evident", "evolution", "exact",
    "excalibur", "excavation", "exonerate", "expect",
    "fabric", "facets", "factor", "fastest", "fathom", "ferret", "fiddler", "fidelity",
    "field", "fig", "figure", "figured", "fiji", "fillet", "filter", "finale",
    "fish", "fishbowl", "flavors", "flex", "flip", "float", "flock", "flow",
    "flywheel", "fmri", "focus", "folio", "forge", "fortify", "foster",
    "foundation", "framer", "frameworks", "freehand", "front", "frostbite", "fuel",
    "fulcrum", "fuze",
    "gaea", "gain", "galaxy", "galena", "galileo", "galley", "ganglia", "gather",
    "gatling", "gauss", "gaussian", "gavel", "gazebo", "gem", "gemini", "genesis",
    "gerber", "gizmos", "glean", "glimmer", "global", "gnupg", "goal", "godot",
    "golly", "gong", "grace", "grads", "granular", "graphite", "grasshopper",
    "gravitation", "greenhouse", "gremlin", "grep", "grin", "grow", "guardian",
    "guide", "guile", "guru", "gusto",
    "haas", "halcyon", "hammer", "handlebars", "handshake", "harbor", "harvest",
    "hazmat", "head", "headmaster", "heap", "help", "highland", "historian",
    "honeybee", "honeywell", "horde", "horizon", "host", "houdini", "hover", "hub",
    "huddle", "hudson", "hydrogen", "hyperion",
    "ibis", "icon", "icy", "igloo", "ignition", "illumine", "illustrations",
    "impact", "improve", "incisive", "include", "indeed", "indra", "influence",
    "inform", "inroads", "insight", "insomnia", "instruments", "insure", "intercom",
    "interpretation", "interpreters", "interrogation", "involve", "ipad", "ironclad",
    "isabelle", "itunes",
    "jackson", "jason", "jekyll", "jobber", "join", "joy", "jubilee", "jumble", "jump",
    "kaleidoscope", "keeper", "key", "keynote", "kindred", "kingdom", "kismet", "kitty",
    "knack",
    "landmark", "landsat", "later", "latitude", "lattice", "launch", "lawson",
    "lazarus", "lead", "leadership", "leaflet", "learn", "legion", "lever", "liberty",
    "libreoffice", "linear", "lingo", "lingoes", "link", "linkage", "liquid", "locus",
    "locust", "longview", "luigi", "lumberjack", "lyx",
    "madeline", "maestro", "magellan", "magic", "magma", "magnify", "majestic",
    "make", "manage", "mandrill", "manifold", "mantra", "map", "maple", "mari",
    "maris", "marquis", "marvel", "massive", "mastercard", "mastermind", "match",
    "materialize", "matrix", "maxima", "maxwell", "maya", "maze", "measure", "medium",
    "medusa", "meet", "meld", "memories", "mercer", "mercurial", "merit", "metal",
    "mews", "micrometers", "millennium", "mimics", "mind", "mingle", "mint",
    "minuteman", "mission", "moat", "model", "moho", "monaco", "mondrian",
    "monograph", "monitor", "monster", "mosaic", "moses", "motion", "motive", "move",
    "mover", "mull", "municipal", "mural", "mutt", "myspace",
    "namely", "narrator", "navigate", "nebula", "need", "needles", "neon", "nerf",
    "newton", "nimble", "noah", "noggin", "norton", "notarize", "note", "nuke",
    "oberon", "octave", "odyssey", "omega", "open", "openoffice", "optimum", "orange",
    "organization", "origin", "oscillators", "oscilloscope", "oscilloscopes", "osiris",
    "otis", "outreach", "oversight", "overture", "own", "ozone",
    "pacemaker", "packer", "paddle", "pages", "paintbrush", "panoply", "papers",
    "paprika", "papyrus", "paradigm", "paradox", "paragon", "parcel", "parthenon",
    "partner", "passer", "passport", "patchworks", "pathway", "peak", "peep",
    "penultimate", "perforce", "perform", "persuasion", "petra", "petrel", "phrase",
    "piazza", "picky", "pilot", "pillow", "ping", "pizzicato", "plan", "planet",
    "planner", "plexus", "plop", "plus", "pocket", "point", "polygraph", "polymath",
    "poser", "post", "postbox", "postmark", "postscript", "potent", "power",
    "precision", "presentation", "press", "preview", "prime", "primer", "process",
    "processing", "proclaim", "procreate", "professional development", "progeny",
    "prolific", "promote", "propel", "propeller", "prophet", "proscribe", "prosper",
    "proteus", "protractor", "prove", "pug", "pulp", "pulse", "push",
    "quest", "quicken", "quill", "quip", "quire", "quorum",
    "radiant", "raiser s edge", "rally", "range", "ranker", "ray", "reach",
    "reaper", "reason", "recall", "recruit", "recruiter", "recruiters", "redcap",
    "reduce", "reflection", "relate", "relativity", "repast", "report", "resolve",
    "resolver", "retain", "retrospect", "reuters", "rev", "reveal", "review",
    "rhapsody", "rhino", "rippling", "rise", "riverbed", "robin", "role", "rook",
    "root", "rosetta", "roslyn", "ruffle", "run",
    "saber", "sabre", "safari", "sage", "sakai", "salmon", "salome", "samba",
    "sanction", "sandman", "sapphire", "saxon", "scale", "schick", "scope", "scouting",
    "screen", "screenplay", "searchlight", "seesaw", "seek", "segment", "seismic",
    "selector", "serpent", "serum", "serve", "seurat", "set", "shape", "share",
    "sherpa", "shift", "shire", "shortcut", "shortcuts", "sibelius", "sift", "signal",
    "signpost", "silo", "sine", "singular", "siren", "skinning", "skyline", "skyward",
    "sling", "smoke", "snap", "sniper", "snort", "soda", "solve", "solver", "sonnet",
    "soot", "source", "sourceforge", "spacy", "spades", "spark", "spartan",
    "spectrum", "spider", "spire", "splash", "splice", "spock", "spoonful",
    "spotlight", "spring", "spry", "sputtering", "squire", "squirrel", "ss",
    "stage", "stages", "stan", "stand", "starfish", "start", "stature", "stella",
    "step", "sterling", "stitch", "stoplight", "storybook", "strand", "strata",
    "stream", "streamline", "stride", "strong", "structure", "subversion", "succeed",
    "summation", "summit", "superhuman", "supernova", "support", "surfer", "surge",
    "surpass", "sustain", "swaps", "swift", "swiftly", "swim", "swing", "switch",
    "switching", "symmetry", "symphony", "synthesizer", "synthesizers",
    "tadpoles", "tally", "tangier", "tango", "taproot", "target", "teal", "team",
    "teammate", "teamwork", "telnet", "tenable", "test", "testable", "tex", "thales",
    "therefore", "thermodynamics", "things", "thrive", "thunderbird", "thunderhead",
    "ticketmaster", "tiling", "timberline", "timpani", "toad", "toast", "toolbox",
    "topaz", "tornado", "tower", "track", "tracers", "tracking", "tracks", "train",
    "transcend", "transliteration", "transmit", "trapeze", "treat", "trellis",
    "trenching", "triggers", "trinity", "trust", "tulip", "turn", "tuxedo", "twitch",
    "ulysses", "unbound", "uncle", "unite", "upkeep", "utilitarianism",
    "vagabond", "value", "vanilla", "vantage", "vellum", "velvet", "vend", "venture",
    "vera", "vertex", "view", "viewpoint", "vignette", "vine", "virtuous", "visa",
    "vision", "visionary", "visit", "vital", "vitals", "vivaldi", "voice", "vortex",
    "voyager", "vulcan",
    "wasp", "waterfall candidate", "watershed", "way", "webmaster", "whimsical",
    "whisper", "win", "wine", "wink", "wisely", "witness", "wombat", "woodpecker",
    "woodstock", "work", "workable", "workday", "wright",
    "xterm", "yacc", "yeoman", "yield", "zeke", "zen", "zephyr", "zest", "zola", "zone",
}

WEIGHT_MIN = 1
WEIGHT_MAX = 5
WEIGHT_BASE = 3

REQUIRED_KEYWORDS = [
    "required", "must have", "must-have", "essential", "mandatory",
    "need", "necessary", "critical", "key requirement", "requirements",
    "qualifications", "minimum", "strong", "proven", "solid", "expert"
]

PREFERRED_KEYWORDS = [
    "preferred", "nice to have", "nice-to-have", "plus", "bonus",
    "desirable", "advantageous", "helpful", "ideally", "optional",
    "a plus", "would be nice", "good to have", "familiarity with"
]

POSITION_BONUS_THRESHOLD = 0.30
POSITION_BONUS = 1
FREQUENCY_BONUS_PER_MENTION = 1
FREQUENCY_BONUS_MAX = 1
REQUIRED_CONTEXT_BONUS = 1
PREFERRED_CONTEXT_PENALTY = 1

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_BASE_DIR = os.path.join(SCRIPT_DIR, "logs")
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_DIR = os.path.join(LOGS_BASE_DIR, TODAY_DATE)
LOG_FILE = os.path.join(LOG_DIR, "skill_extraction.log")

os.makedirs(LOG_DIR, exist_ok=True)

log = logging.getLogger("skill_extraction")
log.setLevel(logging.DEBUG)
log.propagate = False

if not log.handlers:
    console_formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    file_formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    log.addHandler(console_handler)

    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        log.addHandler(file_handler)
    except Exception as e:
        print(f"Could not create log file: {e}")


def normalize_text(text: str) -> str:
    if not text:
        return ""
    if not CASE_SENSITIVE:
        text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[/|]", " ", text)
    text = re.sub(r"[^a-z0-9\.\+\#\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class SkillExtractor:

    def __init__(self):
        self.kp = None
        self.skill_lookup = {}
        self.loaded = False
        self.total_terms = 0
        self.total_skills = 0

    async def load_async(self, db, collection_name: str = None):
        if self.loaded:
            return
        if not MOTOR_AVAILABLE:
            log.error("motor not installed.")
            return

        collection_name = collection_name or SKILLS_COLLECTION
        collection = db[collection_name]
        log.info("Loading from %s.%s (name + aliases)...", db.name, collection_name)

        self.kp = KeywordProcessor(case_sensitive=CASE_SENSITIVE)
        skipped_noisy = 0

        async for doc in collection.find({"isDeleted": {"$ne": True}}, {"_id": 1, "name": 1, "aliases": 1}):
            skill_id = str(doc.get("_id"))
            name = (doc.get("name") or "").strip()
            if not name:
                continue

            name_normalized = normalize_text(name)
            if len(name_normalized) < MIN_TOKEN_LEN:
                continue
            if name_normalized in NOISY_CANONICAL:
                skipped_noisy += 1
                continue

            self.skill_lookup[name_normalized] = {"skill_id": skill_id, "skill_name": name, "display_text": name}
            self.kp.add_keyword(name_normalized, name_normalized)
            self.total_terms += 1

            aliases = doc.get("aliases") or []
            if isinstance(aliases, list):
                for alias in aliases:
                    if not isinstance(alias, str):
                        continue
                    alias = alias.strip()
                    if not alias:
                        continue
                    alias_normalized = normalize_text(alias)
                    if len(alias_normalized) < MIN_TOKEN_LEN or len(alias_normalized) > MAX_ALIAS_LEN:
                        continue
                    if alias_normalized in NOISY_CANONICAL:
                        continue
                    if alias_normalized not in self.skill_lookup:
                        self.skill_lookup[alias_normalized] = {"skill_id": skill_id, "skill_name": name, "display_text": name}
                    self.kp.add_keyword(alias_normalized, alias_normalized)
                    self.total_terms += 1

            self.total_skills += 1

        self.loaded = True
        log.info("Loaded %d skills with %d terms, skipped %d noisy", self.total_skills, self.total_terms, skipped_noisy)

    def load(self):
        if self.loaded:
            return

        if USE_STAGING:
            uri, db_name = STAGING_URI, STAGING_DB
        else:
            uri, db_name = PROD_URI, PROD_DB

        client = MongoClient(uri)
        db = client[db_name]
        self.kp = KeywordProcessor(case_sensitive=CASE_SENSITIVE)
        collection = db[SKILLS_COLLECTION]

        log.info("Loading from %s.%s (name + aliases)...", db.name, SKILLS_COLLECTION)
        skipped_noisy = 0

        for doc in collection.find({"isDeleted": {"$ne": True}}, {"_id": 1, "name": 1, "aliases": 1}):
            skill_id = str(doc.get("_id"))
            name = (doc.get("name") or "").strip()
            if not name:
                continue

            name_normalized = normalize_text(name)
            if len(name_normalized) < MIN_TOKEN_LEN:
                continue
            if name_normalized in NOISY_CANONICAL:
                skipped_noisy += 1
                continue

            self.skill_lookup[name_normalized] = {"skill_id": skill_id, "skill_name": name, "display_text": name}
            self.kp.add_keyword(name_normalized, name_normalized)
            self.total_terms += 1

            aliases = doc.get("aliases") or []
            if isinstance(aliases, list):
                for alias in aliases:
                    if not isinstance(alias, str):
                        continue
                    alias = alias.strip()
                    if not alias:
                        continue
                    alias_normalized = normalize_text(alias)
                    if len(alias_normalized) < MIN_TOKEN_LEN or len(alias_normalized) > MAX_ALIAS_LEN:
                        continue
                    if alias_normalized in NOISY_CANONICAL:
                        continue
                    if alias_normalized not in self.skill_lookup:
                        self.skill_lookup[alias_normalized] = {"skill_id": skill_id, "skill_name": name, "display_text": name}
                    self.kp.add_keyword(alias_normalized, alias_normalized)
                    self.total_terms += 1

            self.total_skills += 1

        client.close()
        self.loaded = True
        log.info("Loaded %d skills with %d terms, skipped %d noisy", self.total_skills, self.total_terms, skipped_noisy)

    def extract(self, text: str) -> Tuple[List[Dict], List[Dict]]:
        if not self.loaded or not self.kp:
            log.warning("Extractor not loaded. Call load() first.")
            return [], []

        normalized = normalize_text(text)
        text_length = len(normalized)
        position_threshold = int(text_length * POSITION_BONUS_THRESHOLD)

        required_zones = self._find_context_zones(normalized, REQUIRED_KEYWORDS)
        preferred_zones = self._find_context_zones(normalized, PREFERRED_KEYWORDS)

        if RETURN_SPANS:
            matches = self.kp.extract_keywords(normalized, span_info=True)
        else:
            matches = self.kp.extract_keywords(normalized)

        skill_matches = {}
        details = []

        if RETURN_SPANS:
            for term, start, end in matches:
                term_lower = term.lower()
                if term_lower in NOISY_CANONICAL:
                    continue
                skill_info = self.skill_lookup.get(term_lower)
                if not skill_info:
                    continue
                skill_id = skill_info.get("skill_id")
                if not skill_id:
                    continue
                if skill_id not in skill_matches:
                    skill_matches[skill_id] = {"info": skill_info, "term": term, "positions": []}
                skill_matches[skill_id]["positions"].append((start, end))
                details.append({"canonical": skill_info.get("skill_name") or term, "matched": normalized[start:end], "start": start, "end": end, "skill_id": skill_id})
        else:
            for term in matches:
                term_lower = term.lower()
                if term_lower in NOISY_CANONICAL:
                    continue
                skill_info = self.skill_lookup.get(term_lower)
                if not skill_info:
                    continue
                skill_id = skill_info.get("skill_id")
                if not skill_id:
                    continue
                if skill_id not in skill_matches:
                    skill_matches[skill_id] = {"info": skill_info, "term": term, "positions": []}
                skill_matches[skill_id]["positions"].append((0, 0))

        unique_skills = []

        for skill_id, data in skill_matches.items():
            skill_info = data["info"]
            positions = data["positions"]
            term = data["term"]
            weight = WEIGHT_BASE

            frequency = len(positions)
            if frequency > 1:
                weight += min((frequency - 1) * FREQUENCY_BONUS_PER_MENTION, FREQUENCY_BONUS_MAX)

            first_position = min(pos[0] for pos in positions) if positions else text_length
            if first_position < position_threshold:
                weight += POSITION_BONUS

            is_required = True
            has_required_context = False
            has_preferred_context = False

            for start, end in positions:
                if self._is_in_context_zone(start, end, required_zones):
                    has_required_context = True
                if self._is_in_context_zone(start, end, preferred_zones):
                    has_preferred_context = True

            if has_required_context and not has_preferred_context:
                weight += REQUIRED_CONTEXT_BONUS
                is_required = True
            elif has_preferred_context and not has_required_context:
                weight -= PREFERRED_CONTEXT_PENALTY
                is_required = False
            elif has_required_context and has_preferred_context:
                weight += 1
                is_required = True

            weight = max(WEIGHT_MIN, min(WEIGHT_MAX, weight))

            unique_skills.append({
                "skill_id": skill_id,
                "name": skill_info.get("display_text") or skill_info.get("skill_name") or term,
                "display_text": skill_info.get("display_text") or term,
                "weight": weight,
                "required": is_required,
            })

        unique_skills.sort(key=lambda x: (-x["weight"], x["name"]))
        return unique_skills, details

    def _find_context_zones(self, text: str, keywords: List[str], window: int = 100) -> List[Tuple[int, int]]:
        zones = []
        text_lower = text.lower()
        for keyword in keywords:
            kw = keyword.lower().replace("-", " ")
            start = 0
            while True:
                idx = text_lower.find(kw, start)
                if idx == -1:
                    break
                zone_start = max(0, idx - window)
                zone_end = min(len(text), idx + len(kw) + window)
                zones.append((zone_start, zone_end))
                start = idx + 1
        if not zones:
            return []
        zones.sort()
        merged = [zones[0]]
        for zone_start, zone_end in zones[1:]:
            last_start, last_end = merged[-1]
            if zone_start <= last_end:
                merged[-1] = (last_start, max(last_end, zone_end))
            else:
                merged.append((zone_start, zone_end))
        return merged

    def _is_in_context_zone(self, start: int, end: int, zones: List[Tuple[int, int]]) -> bool:
        for zone_start, zone_end in zones:
            if start < zone_end and end > zone_start:
                return True
        return False

    def extract_bulk(self, texts: List[str], include_details: bool = False) -> Dict[int, List[Dict]]:
        results = {}
        for idx, text in enumerate(texts):
            skills, details = self.extract(text)
            if include_details:
                results[idx] = (skills, details)
            else:
                results[idx] = skills
        return results

    def match_skills_bulk(self, texts: List[str]) -> Dict[int, List[Dict]]:
        return self.extract_bulk(texts, include_details=False)

    def match_skills_from_text(self, text: str) -> List[Dict]:
        skills, _ = self.extract(text)
        return skills


# if __name__ == "__main__":
#     print(f"Log file: {LOG_FILE}")
#     extractor = SkillExtractor()
#     extractor.load()

#     test_jds = [
#         "Senior Software Engineer - Python is essential. ReactJS must have. AWS required. Docker is a plus. Kubernetes nice to have.",
#         "Data Scientist - Machine Learning mandatory. SQL required. TensorFlow necessary. PyTorch nice to have. Tableau preferred.",
#         "Product Manager - Agile essential. JIRA must be proficient. Scrum required. SQL helpful but optional.",
#     ]

#     for i, jd in enumerate(test_jds):
#         skills, details = extractor.extract(jd)
#         print(f"\nJD {i+1}: {len(skills)} skills")
#         for s in skills[:10]:
#             print(f"  {s['display_text']:<25} weight={s['weight']} required={s['required']}")

#     print("\nDone!")
