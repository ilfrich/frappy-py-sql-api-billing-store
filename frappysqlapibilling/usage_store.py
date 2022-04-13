from datetime import datetime, time
from typing import Union, Optional, List
from frappyapibilling import AbstractUsageStore, Usage
from flask_sqlalchemy import SQLAlchemy, DefaultMeta, BaseQuery
from sqlalchemy import func
from sqlalchemy.orm import scoped_session


def _create_sql_model(db: SQLAlchemy) -> DefaultMeta:
    # register class
    class UsageModel(db.Model):
        id = db.Column(db.Integer, primary_key=True)
        client_id = db.Column(db.String)
        timestamp = db.Column(db.Integer)
        credits = db.Column(db.Float)

    return UsageModel


class UsageStore(AbstractUsageStore):

    def __init__(self, sql_db: SQLAlchemy):
        self.model: DefaultMeta = _create_sql_model(sql_db)
        self.sql_db: SQLAlchemy = sql_db

    def track_usage(self, client_id: Union[str, int], credits_used: Union[int, float]):
        session = self._get_session()
        # create new instance
        new_item = self.model(client_id=str(client_id), timestamp=round(datetime.now().timestamp()),
                              credits=credits_used)
        # add and commit
        session.add(new_item)
        session.commit()

    def get_total_usage(self, client_id: Union[str, int], start_datetime: datetime,
                        end_datetime: datetime) -> Union[float, int]:
        # compose query
        res = self._filter_all(self._get_session().query(func.sum(self.model.credits)), client_id, start_datetime,
                               end_datetime)
        # fetch the first result
        query_result = res.first()
        # check if it exists
        if query_result is None:
            return 0
        # return sum of credits used
        return query_result[0]

    def get_daily_usage(self, client_id: Union[str, int], start_datetime: datetime,
                        end_datetime: datetime) -> List[Usage]:
        # compose query
        items = self._filter_all(self.model.query, client_id, start_datetime, end_datetime)
        # process query result
        result_map = {}
        for item in items.all():
            # get timestamp
            ts = round(datetime.combine(datetime.fromtimestamp(item.timestamp).date(), time()).timestamp())
            if ts not in result_map:
                # create new usage element
                new_usage = Usage()
                new_usage.timestamp = ts
                new_usage.client_id = client_id
                result_map[ts] = new_usage

            # increment by used credits
            result_map[ts].credits += item.credits

        # return sorted (by timestamp) list of usage elements
        return list(sorted(result_map.values(), key=lambda x: x.timestamp))

    def delete_client_usage(self, client_id: Union[str, int], opt_start_dt: Optional[datetime] = None,
                            opt_end_dt: Optional[datetime] = None):
        session = self._get_session()
        # compose query
        res = self._filter_all(session.query, client_id, opt_start_dt, opt_end_dt)
        # delete all items affected and commit
        res.delete()
        session.commit()

    def _get_session(self) -> scoped_session:
        return self.sql_db.session

    def _filter_all(self, base_query: BaseQuery, client_id: Union[str, int], start_datetime: Optional[datetime],
                    end_datetime: Optional[datetime]) -> BaseQuery:
        return self._filter_end_date(self._filter_start_date(self._filter_client_id(
            base_query, client_id), start_datetime), end_datetime)

    def _filter_client_id(self, query_result: BaseQuery, client_id: Union[int, str]) -> BaseQuery:
        return query_result.filter(self.model.client_id == str(client_id))

    def _filter_start_date(self, query_result: BaseQuery, start_datetime: Optional[datetime]) -> BaseQuery:
        if start_datetime is None:
            return query_result

        return query_result.filter(self.model.timestamp >= round(start_datetime.timestamp()))

    def _filter_end_date(self, query_result: BaseQuery, end_datetime: Optional[datetime]) -> BaseQuery:
        if end_datetime is None:
            return query_result
        return query_result.filter(self.model.timestamp <= round(end_datetime.timestamp()))
