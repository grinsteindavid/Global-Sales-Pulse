import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ..models import Transaction
from ..transformations.transformer import TransformedTransaction

logger = logging.getLogger(__name__)


class PostgresLoader:
    def __init__(self, session: Session):
        self.session = session

    def load_batch(self, transactions: list[TransformedTransaction]) -> int:
        if not transactions:
            logger.info("No transactions to load")
            return 0

        records = [t.model_dump() for t in transactions]

        stmt = insert(Transaction).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["order_id"])

        result = self.session.execute(stmt)
        self.session.commit()

        loaded_count = result.rowcount if result.rowcount else len(records)
        logger.info(f"Loaded {loaded_count} transactions to warehouse")
        return loaded_count
