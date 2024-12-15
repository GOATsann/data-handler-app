import datetime
import json
import pytz
from aws_lambda_powertools.event_handler import APIGatewayRestResolver, CORSConfig
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools import Logger
from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Metrics

from stock_crypto_data.fmp_data_handler import (
    FMPStockCryptoDataRetriever,
    parse_date_string,
)
from indicator_handler.talib_handler import TaLibIndicatorHandler
from utils import GeneralEncoder, get_last_n_points

cors_config = CORSConfig(
    allow_origin="*",
    extra_origins=[],
    allow_headers=["*"],
    expose_headers=[],
    max_age=6000,
)

app = APIGatewayRestResolver(cors=cors_config)
tracer = Tracer()
logger = Logger()
metrics = Metrics(namespace="Powertools")

fmp_handler = FMPStockCryptoDataRetriever()
talib_handler = TaLibIndicatorHandler()


@app.post("/get_data/")
@tracer.capture_method
def get_data():
    timezone = pytz.timezone("America/New_York")
    current_time = datetime.datetime.now(timezone)
    current_json = app.current_event.json_body

    type_ = current_json.get("data_type")  # stock, or crypto
    asset_symbol = current_json.get("data_name")  # stock name, or crypto name
    from_date = current_json.get("from_date")
    to_date = current_json.get("to_date", current_time.strftime("%Y-%m-%d %H:%M:%S%z"))
    tframe = current_json.get("time_frame", "1day")

    start_date = parse_date_string(from_date)
    end_date = parse_date_string(to_date)
    retrieved_data = fmp_handler.retrieve_historical_bars(
        asset_symbol, start_date, end_date, tframe, type_
    )
    return json.dumps({"data": retrieved_data})


@app.post("/get_indicator_data/")
@tracer.capture_method
def get_indicator_data():
    current_json = app.current_event.json_body
    name = current_json.get("indicator_name")
    asset_symbol = current_json.get("source_name")
    tframe = current_json.get("time_frame", "1day")
    type_ = current_json.get("data_type")  # stock, or crypto
    start_date = parse_date_string(current_json.get("from_date"))
    end_date = parse_date_string(
        current_json.get(
            "to_date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S%z")
        )
    )
    kwargs = current_json.get("kwargs", {})
    # as we return floats as strings, we need to check for this case incase the client returns default values
    for key in kwargs:
        if isinstance(kwargs[key], str) and kwargs[key].replace(".", "").isdigit():
            kwargs[key] = float(kwargs[key])

    # this is ok because we are low volume. But if we were high volume,
    # we would need to use a cache, or store in a database once retrieved,
    # depending on what stocks are currently being viewd on our site
    source_data = fmp_handler.retrieve_historical_bars(
        asset_symbol,
        start_date,
        end_date,
        tframe,
        type_,
        format_taLib=True,
    )

    data = talib_handler.get_indicator(
        indicator_name=name, inputs=source_data, **kwargs
    )

    # only return a max of 10K data points
    data = get_last_n_points(data)
    return json.dumps({"data": data}, cls=GeneralEncoder)


@app.get("/get_available_indicators/")
@tracer.capture_method
def get_available_indicators():
    data = talib_handler.get_lib_descriptions()
    return json.dumps({"data": data})


# Enrich logging with contextual information from Lambda
@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
# Adding tracer
# See: https://awslabs.github.io/aws-lambda-powertools-python/latest/core/tracer/
@tracer.capture_lambda_handler(capture_response=False)
# ensures metrics are flushed upon request completion/failure and capturing ColdStart metric
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)
