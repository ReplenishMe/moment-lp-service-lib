import os

from dotenv import load_dotenv

from momenttrack_shared_services import LicensePlateServiceAgent

load_dotenv()


WRITER_DB_URI = os.getenv("DATABASE_URL_WRITER", "sqlite:///dev.db")
DATABASE_URL_REFRESHER = os.getenv("DATABASE_URL_REFRESHER", "sqlite:///dev.db")
conf = {
    'SQLALCHEMY_DATABASE_URI': os.getenv('DATABASE_URL'),
    'SQLALCHEMY_BINDS': {
        "writer": WRITER_DB_URI,
        "cache_refresher": DATABASE_URL_REFRESHER
    }
}
agent = LicensePlateServiceAgent(conf)

lp_move = agent.move(
    2202, 358, 4, {}, 2
)
print(lp_move)
