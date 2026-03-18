from __future__ import annotations

from dataclasses import dataclass
from faker import Faker
from random import choice, random
from uuid import uuid4

fake = Faker("en_US")

@dataclass
class Customer:
    customer_id: str
    transformer_id: str
    customer_class: str   # RES | COM | IND
    first_name: str
    last_name: str
    phone: str
    email: str
    critical_flag: str | None = None

def _create_phone() -> str:
  return fake.numerify(text="##########")

def _create_customer_id() -> str:
  return str(uuid4())

def _weighted_pick(values: list[int]) -> int:
  return choice(values)

def _allowed_classes(kva: float) -> list[str]:
  if kva < 100:
    return ["RES"]
  if kva < 300:
    return ["RES", "COM"]
  if kva < 500:
    return ["COM", "RES"]
  return ["IND"]

def _create_res_customer(transformer_id: str) -> Customer:
  critical_flag: str | None = None

  # modest chance of a special residential customer
  if random() < 0.01:
    critical_flag = choice(["medical", "hospice", "life_support"])

  return Customer(
    customer_id=_create_customer_id(),
    transformer_id=transformer_id,
    customer_class="RES",
    first_name=fake.first_name(),
    last_name=fake.last_name(),
    phone=_create_phone(),
    email=fake.email(),
    critical_flag=critical_flag,
  )

def _create_com_customer(transformer_id: str) -> Customer:
  critical_flag : str | None = None

  # approximately 1:500 chance
  if random() < (1 / 500):
    critical_flag = choice(["hospital", "ems", "police", "fire_station"])

  return Customer(
    customer_id=_create_customer_id(),
    transformer_id=transformer_id,
    customer_class="COM",
    first_name=fake.first_name(),
    last_name=fake.last_name(),
    phone=_create_phone(),
    email=fake.company_email(),
    critical_flag=critical_flag,
  )

def _create_ind_customer(transformer_id: str) -> Customer:
  return Customer(
    customer_id=_create_customer_id(),
    transformer_id=transformer_id,
    customer_class="IND",
    first_name=fake.first_name(),
    last_name=fake.last_name(),
    phone=_create_phone(),
    email=fake.company_email(),
    critical_flag=None,
  )

def _res_count_for_kva(kva: float) -> int:
  if kva < 25:
    return _weighted_pick([1, 1, 1, 1, 1, 2, 2, 2, 3, 4])
  if kva < 50:
    return _weighted_pick([1, 1, 1, 2, 2, 2, 3, 3, 4, 5, 6])
  if kva < 100:
    return _weighted_pick([1, 1, 2, 2, 3, 3, 4, 4, 5, 6, 8])
  if kva < 300:
    return _weighted_pick([1, 2, 2, 3, 3, 4, 4, 5, 6, 8, 10, 12])
  return 0

def _com_count_for_kva(kva: float) -> int:
  if kva < 100:
    return 0
  if kva < 300:
    return _weighted_pick([0, 0, 0, 1, 1, 1, 1, 2, 2, 3])
  if kva < 500:
    return _weighted_pick([1, 1, 1, 1, 2, 2, 2, 3, 3, 4, 5])
  return 0

def _ind_count_for_kva(kva: float) -> int:
  return 1 if kva >= 500 else 0


def create_customers(transformer_id: str, kva: float) -> list[Customer]:
  customers: list[Customer] = []
  allowed = _allowed_classes(kva)

  if "IND" in allowed:
    for _ in range(_ind_count_for_kva(kva)):
      customers.append(_create_ind_customer(transformer_id))
    return customers

  if "RES" in allowed:
    for _ in range(_res_count_for_kva(kva)):
      customers.append(_create_res_customer(transformer_id))

  if "COM" in allowed:
    for _ in range(_com_count_for_kva(kva)):
      customers.append(_create_com_customer(transformer_id))

  return customers