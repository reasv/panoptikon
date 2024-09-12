from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.pql_model import (
    AndOperator,
    NotOperator,
    Operator,
    OrOperator,
    QueryElement,
)


def preprocess_query(el: QueryElement) -> QueryElement | None:
    """Validates all individual filters, and removes empty ones so they
    are skipped in the final query.
    Args:
        el (QueryElement): The query element to preprocess

    Raises:
        ValueError: Filters will raise a ValueError if their args are invalid

    Returns:
        QueryElement | None: The validated query element or None if it is empty
    """
    if isinstance(el, Filter):
        return el.validate()
    elif isinstance(el, Operator):
        if isinstance(el, AndOperator):
            element_list = []
            for sub_element in el.and_:
                subquery = preprocess_query(sub_element)
                if subquery:
                    element_list.append(subquery)
            if not element_list:
                return None
            if len(element_list) == 1:
                return element_list[0]
            return AndOperator(and_=element_list)

        elif isinstance(el, OrOperator):
            element_list = []
            for sub_element in el.or_:
                subq = preprocess_query(sub_element)
                if subq:
                    element_list.append(subq)
            if not element_list:
                return None
            if len(element_list) == 1:
                return element_list[0]
            return OrOperator(or_=element_list)

        elif isinstance(el, NotOperator):
            subquery = preprocess_query(el.not_)
            if subquery:
                return NotOperator(not_=subquery)
            return None
    else:
        raise ValueError("Unknown query element type")
