import json

from src.utils import logging

def normalize_scalar(ftype):
    type_mapping = {
        "BOOLEAN": bool,
        "DATE": str,
        "FLOAT": float,
        "INTEGER": int,
        "VARCHAR": str,
        "TIMESTAMP": str,
        "TIME": str,
        "JSON_STRING": str
    }

    def converter(value):
        if value in [None, '']:
            return None
        else:
            return type_mapping[ftype](value)

    return converter


class _ListNormalizer:
    def __init__(self, field):
        self._field = field
        self._normalizer = None
        self._generate_normalizer()

    def _generate_normalizer(self):
        ftype = self._field['type']
        if ftype == 'RECORD':
            record_normalizer = _RecordNormalizer(self._field['fields'])
            self._normalizer = record_normalizer.normalize
        else:
            try:
                self._normalizer = normalize_scalar(ftype)
            except KeyError:
                raise ValueError(f'Invalid field type: {ftype}')

    def normalize(self, data):
        if self._field['type'] == "JSON_STRING":
            try:
                data = json.loads(data)
            except Exception:
                data = []

        if isinstance(data, str) or data == []:
            return []
        else:
            # Need to remove None values in list because BQ doesn't allow None value inside a list
            data = list(filter(lambda x: x not in [None, ''], data))
            return [self._normalizer(it) for it in data]


class _RecordNormalizer:
    def __init__(self, schema):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._normalizer = dict()
        self._schema = schema
        self._generate_normalizer()

    def _generate_normalizer(self):
        for field in self._schema:
            fname = field.get('name')
            ftype = field.get('type')
            fmode = field.get('mode')
            if fmode in ["NULLABLE", "REQUIRED"]:
                if ftype == 'RECORD':
                    record_normalizer = _RecordNormalizer(field['fields'])
                    self._normalizer[fname] = record_normalizer.normalize
                else:
                    try:
                        self._normalizer[fname] = normalize_scalar(ftype)
                    except KeyError:
                        raise ValueError(f'Invalid field type: {ftype}')
            elif fmode == "REPEATED":
                list_normalizer = _ListNormalizer(field)
                self._normalizer[fname] = list_normalizer.normalize
            else:
                raise ValueError(f'Invalid field mode: {fmode}')

    def normalize(self, record):
        if isinstance(record, str):
            record = json.loads(record)
        result = dict()
        if record:
            for k, v in record.items():
                try:
                    result[k] = self._normalizer[k](v)
                except KeyError:
                    pass
        return result


class NoSqlNormalizer:
    def __init__(self, schema):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._schema = schema
        self._normalizer = dict()
        self._result = list()
        self._generate_normalizer()

    def _generate_normalizer(self):
        self._logger.info(f'Generate normalizer.')
        for field in self._schema:
            fname = field.get('name')
            ftype = field.get('type')
            fmode = field.get('mode')
            if fmode in ["NULLABLE", "REQUIRED"]:
                if ftype == 'RECORD':
                    record_normalizer = _RecordNormalizer(field['fields'])
                    self._normalizer[fname] = record_normalizer.normalize
                else:
                    try:
                        self._normalizer[fname] = normalize_scalar(ftype)
                    except KeyError:
                        raise ValueError(f'Invalid field type: {ftype}')
            elif fmode == "REPEATED":
                list_normalizer = _ListNormalizer(field)
                self._normalizer[fname] = list_normalizer.normalize
            else:
                raise ValueError(f'Invalid field mode: {fmode}')

    def _normalize(self, name):
        return self._normalizer[name]

    def normalize_data(self, data):
        self._result = list()
        for rec in data:
            try:
                normalized_rec = dict()
                for k, v in rec.items():
                    try:
                        normalized_rec[k] = self._normalize(k)(v)
                    except TypeError as e:
                        self._logger.warning(f'TypeError when normalized {rec}')
                        self._logger.warning(f'key-value: dict({k}: {v})')
                        self._logger.warning(e)
                        normalized_rec[k] = None
                    except ValueError as e:
                        self._logger.warning(f'ValueError when normalized {rec}')
                        self._logger.warning(f'key-value: dict({k}: {v})')
                        self._logger.warning(e)
                        normalized_rec[k] = None
                    except KeyError:
                        pass
                    except Exception as e:
                        self._logger.error(f'error when normalized {rec}')
                        self._logger.error(f'key-value: dict({k}: {v})')
                        self._logger.error(e)
                        raise
                self._result.append(normalized_rec)
            except Exception as e:
                self._logger.error(e)
                raise

    @property
    def result(self) -> list:
        return self._result
