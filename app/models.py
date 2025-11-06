# SQLAlchemy models for DB Monitor Server
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, Text

Base = declarative_base()

class KafkaEvent(Base):
	__tablename__ = "kafka_events"
	id = Column(Integer, primary_key=True, index=True)
	value = Column(Text, nullable=False)


# Table design, rename to Events and capture more fields instead of saving the whole json string
# vals to extract; event_type, event_time, user_id, event_data
# non event data fields; service_name, 
