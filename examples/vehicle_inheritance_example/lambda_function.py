from fastsqs import FastSQS, SQSRouter, SQSEvent, LoggingMiddleware
import json
from typing import Literal


# Base Vehicle Event
class Vehicle(SQSEvent):
    """Base class for all vehicle events"""

    vehicle_id: str
    location: str
    timestamp: str = None


# Specific Vehicle Types
class Car(Vehicle):
    """Car-specific event with additional car parameters"""

    fuel_level: float  # 0.0 to 1.0
    mileage: int
    engine_type: Literal["gasoline", "diesel", "hybrid", "electric"] = "gasoline"


class Bus(Vehicle):
    """Bus-specific event with additional bus parameters"""

    passenger_count: int
    route_number: str
    capacity: int = 50
    accessibility_enabled: bool = True


class Truck(Vehicle):
    """Truck-specific event with additional truck parameters"""

    cargo_weight: float  # in tons
    cargo_type: str
    max_weight: float = 40.0  # max legal weight in tons
    trailer_attached: bool = True


class Motorcycle(Vehicle):
    """Motorcycle-specific event with additional motorcycle parameters"""

    engine_cc: int  # engine displacement
    license_type: Literal["A", "A1", "A2"] = "A"
    helmet_detected: bool = True


# Main FastSQS App
app = FastSQS(
    debug=True,
)

# Create a vehicle router that handles all vehicle types
# Pass the base Vehicle class to the router
vehicle_router = SQSRouter(Vehicle)

# Add middleware
app.add_middleware(LoggingMiddleware())

# Include the vehicle router
app.include_router(vehicle_router)


# Vehicle-specific handlers
@vehicle_router.route(Car)
async def handle_car_event(msg: Car):
    print(f"[CAR] Vehicle {msg.vehicle_id} at {msg.location}")
    print(
        f"      Fuel: {msg.fuel_level*100:.1f}%, Mileage: {msg.mileage}km, Engine: {msg.engine_type}"
    )
    return {
        "status": "success",
        "vehicle_type": "car",
        "vehicle_id": msg.vehicle_id,
        "fuel_percentage": msg.fuel_level * 100,
        "engine_type": msg.engine_type,
    }


@vehicle_router.route(Bus)
async def handle_bus_event(msg: Bus):
    print(f"[BUS] Vehicle {msg.vehicle_id} at {msg.location}")
    print(
        f"      Route: {msg.route_number}, Passengers: {msg.passenger_count}/{msg.capacity}"
    )
    print(
        f"      Accessibility: {'Enabled' if msg.accessibility_enabled else 'Disabled'}"
    )
    return {
        "status": "success",
        "vehicle_type": "bus",
        "vehicle_id": msg.vehicle_id,
        "occupancy_rate": (msg.passenger_count / msg.capacity) * 100,
        "route": msg.route_number,
    }


@vehicle_router.route(Truck)
async def handle_truck_event(msg: Truck):
    print(f"[TRUCK] Vehicle {msg.vehicle_id} at {msg.location}")
    print(
        f"        Cargo: {msg.cargo_weight}t of {msg.cargo_type} (max: {msg.max_weight}t)"
    )
    print(f"        Trailer: {'Attached' if msg.trailer_attached else 'Detached'}")

    # Check if overweight
    is_overweight = msg.cargo_weight > msg.max_weight

    return {
        "status": "success",
        "vehicle_type": "truck",
        "vehicle_id": msg.vehicle_id,
        "cargo_weight": msg.cargo_weight,
        "cargo_type": msg.cargo_type,
        "is_overweight": is_overweight,
        "weight_utilization": (msg.cargo_weight / msg.max_weight) * 100,
    }


@vehicle_router.route(Motorcycle)
async def handle_motorcycle_event(msg: Motorcycle):
    print(f"[MOTORCYCLE] Vehicle {msg.vehicle_id} at {msg.location}")
    print(f"             Engine: {msg.engine_cc}cc, License: {msg.license_type}")
    print(
        f"             Helmet: {'Detected' if msg.helmet_detected else 'NOT DETECTED - SAFETY ALERT!'}"
    )

    return {
        "status": "success",
        "vehicle_type": "motorcycle",
        "vehicle_id": msg.vehicle_id,
        "engine_cc": msg.engine_cc,
        "license_type": msg.license_type,
        "safety_compliant": msg.helmet_detected,
    }


# You can also have a general vehicle handler for base Vehicle events
@vehicle_router.route(Vehicle)
async def handle_general_vehicle_event(msg: Vehicle):
    print(f"[VEHICLE] General vehicle {msg.vehicle_id} at {msg.location}")
    return {
        "status": "success",
        "vehicle_type": "generic",
        "vehicle_id": msg.vehicle_id,
        "location": msg.location,
    }


def lambda_handler(event, context):
    return app.handler(event, context)


def test_vehicle_inheritance():
    print("=== Testing Vehicle Inheritance Routing ===")

    test_cases = [
        # Car events
        {
            "type": "car",
            "vehicle_id": "CAR-001",
            "location": "Downtown Parking",
            "fuel_level": 0.75,
            "mileage": 45000,
            "engine_type": "hybrid",
        },
        {
            "type": "car",
            "vehicle_id": "CAR-002",
            "location": "Highway Rest Stop",
            "fuel_level": 0.15,
            "mileage": 120000,
            "engine_type": "electric",
        },
        # Bus events
        {
            "type": "bus",
            "vehicle_id": "BUS-101",
            "location": "Central Station",
            "passenger_count": 35,
            "route_number": "Route 42",
            "capacity": 60,
            "accessibility_enabled": True,
        },
        {
            "type": "bus",
            "vehicle_id": "BUS-102",
            "location": "School District",
            "passenger_count": 28,
            "route_number": "School Route A",
            "capacity": 40,
            "accessibility_enabled": False,
        },
        # Truck events
        {
            "type": "truck",
            "vehicle_id": "TRUCK-501",
            "location": "Loading Dock",
            "cargo_weight": 35.5,
            "cargo_type": "Electronics",
            "max_weight": 40.0,
            "trailer_attached": True,
        },
        {
            "type": "truck",
            "vehicle_id": "TRUCK-502",
            "location": "Weigh Station",
            "cargo_weight": 45.0,  # Overweight!
            "cargo_type": "Construction Materials",
            "max_weight": 40.0,
            "trailer_attached": True,
        },
        # Motorcycle events
        {
            "type": "motorcycle",
            "vehicle_id": "MOTO-201",
            "location": "City Center",
            "engine_cc": 600,
            "license_type": "A",
            "helmet_detected": True,
        },
        {
            "type": "motorcycle",
            "vehicle_id": "MOTO-202",
            "location": "Suburban Street",
            "engine_cc": 125,
            "license_type": "A1",
            "helmet_detected": False,  # Safety violation!
        },
        # Generic vehicle event
        {
            "type": "vehicle",
            "vehicle_id": "UNKNOWN-999",
            "location": "Maintenance Yard",
            "timestamp": "2024-01-15T14:30:00Z",
        },
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- Test Case {i}: {test_case['type'].upper()} ---")
        event = {
            "Records": [
                {
                    "messageId": f"msg-{i}",
                    "body": json.dumps(test_case),
                    "attributes": {},
                }
            ]
        }

        result = lambda_handler(event, None)
        print(f"Result: {result}")


if __name__ == "__main__":
    test_vehicle_inheritance()
