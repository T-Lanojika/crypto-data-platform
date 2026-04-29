from dataclasses import dataclass


@dataclass
class GapClassification:
    fill_method: str
    is_interpolated: bool
    confidence_score: float


class GapClassifierService:
    """Classifies gap fill strategy using neighbor prices and gap size."""

    @staticmethod
    def classify(
        start_price: float,
        end_price: float,
        missing_count: int,
        interval_ms: int,
        unreliable_threshold_hours: int,
    ) -> GapClassification:
        gap_duration_ms = missing_count * interval_ms
        threshold_ms = max(1, unreliable_threshold_hours) * 60 * 60 * 1000

        if gap_duration_ms > threshold_ms:
            return GapClassification(
                fill_method="unreliable_gap",
                is_interpolated=False,
                confidence_score=0.1,
            )

        if abs(start_price - end_price) < 1e-12:
            return GapClassification(
                fill_method="flat_fill",
                is_interpolated=True,
                confidence_score=0.9,
            )

        return GapClassification(
            fill_method="linear_interpolation",
            is_interpolated=True,
            confidence_score=0.6,
        )
