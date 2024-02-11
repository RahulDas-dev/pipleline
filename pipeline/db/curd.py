import logging
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import desc, insert, select
from sqlalchemy.orm import Session

from .orm import Results

logger = logging.getLogger(__name__)


def get_all_results(
    session: Session, id_no: int, limit: int = 10
) -> Optional[List[Results]]:
    stmt = (
        select(Results)
        .where(Results.id == id_no)
        .order_by(desc(Results.updated_at))
        .limit(limit)
    )
    results, data = None, None
    try:
        results = session.execute(stmt)
    except Exception as err:
        session.rollback()
        logger.error(
            f"Error While Fteching record from Results Table , ID {id_no}, Error : {err}"
        )
    else:
        logger.info(f"Sucessfully Featched for id {id_no}")
        data = results.scalars().all()
    return data


def add_a_new_results(session: Session, insert_dict: Dict) -> Optional[Results]:
    insert_dict_ = {
        **insert_dict,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    stmt = insert(Results).values(**insert_dict_).returning(Results)
    results, data = None, None
    try:
        results = session.execute(stmt)
    except Exception as err:
        session.rollback()
        logger.error(f"Error While inserting record from Results Table, Error : {err}")
    else:
        logger.info("Sucessfully Inserted results")
        data = results.scalars().one()
    return data
