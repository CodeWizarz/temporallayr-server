import uuid
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.auth import verify_api_key
from app.rules.models import RuleSchema, RuleCreateRequest
from app.rules.store import rule_store

router = APIRouter(tags=["Rules"])


@router.post("/rules", response_model=RuleSchema, status_code=201)
async def create_rule(
    payload: RuleCreateRequest, api_key: str = Depends(verify_api_key)
):
    """Implement fully scalable rule structures persistently natively mapping cache."""
    rule_data = payload.model_dump()
    result = await rule_store.add_rule(tenant_id=api_key, rule_data=rule_data)
    if not result:
        raise HTTPException(
            status_code=500, detail="Failed to create detection rule natively."
        )
    return result


@router.get("/rules", response_model=List[RuleSchema], status_code=200)
async def list_rules(api_key: str = Depends(verify_api_key)):
    """Fetch enabled tenant detection rules mapping memory cache successfully."""
    return await rule_store.get_rules_for_tenant(tenant_id=api_key)


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: uuid.UUID, api_key: str = Depends(verify_api_key)):
    """Safely clear detection rules from DB securely evicting cache traces."""
    success = await rule_store.delete_rule(tenant_id=api_key, rule_id=rule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Rule not found or unauthorized.")
    return None
