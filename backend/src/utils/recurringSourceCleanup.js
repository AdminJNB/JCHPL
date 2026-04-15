const deactivateRecurringRevenuesForBillingRows = async (dbClient, billingRowIds = [], updatedBy = null) => {
  const targetIds = (Array.isArray(billingRowIds) ? billingRowIds : []).filter(Boolean);
  if (!targetIds.length) return 0;

  const result = await dbClient.query(`
    UPDATE revenues
    SET is_active = false,
        updated_at = NOW(),
        updated_by = COALESCE($2::uuid, updated_by)
    WHERE is_active = true
      AND source_type = 'recurring'
      AND client_billing_row_id = ANY($1::uuid[])
  `, [targetIds, updatedBy]);

  return result.rowCount;
};

const deactivateTeamRecurringExpensesForAllocations = async (dbClient, allocationIds = [], updatedBy = null) => {
  const targetIds = (Array.isArray(allocationIds) ? allocationIds : []).filter(Boolean);
  if (!targetIds.length) return 0;

  const result = await dbClient.query(`
    UPDATE expenses
    SET is_active = false,
        updated_at = NOW(),
        updated_by = COALESCE($2::uuid, updated_by)
    WHERE is_active = true
      AND source_type = 'team-recurring'
      AND team_client_allocation_id = ANY($1::uuid[])
  `, [targetIds, updatedBy]);

  return result.rowCount;
};

const cleanupStaleRecurringRevenueRecords = async (dbClient, updatedBy = null) => {
  const result = await dbClient.query(`
    UPDATE revenues r
    SET is_active = false,
        updated_at = NOW(),
        updated_by = COALESCE($1::uuid, r.updated_by)
    WHERE r.is_active = true
      AND r.source_type = 'recurring'
      AND r.client_billing_row_id IS NOT NULL
      AND (
        NOT EXISTS (
          SELECT 1
          FROM client_billing_rows cbr
          WHERE cbr.id = r.client_billing_row_id
        )
        OR EXISTS (
          SELECT 1
          FROM client_billing_rows cbr
          LEFT JOIN service_periods sp ON sp.id = r.service_period_id
          WHERE cbr.id = r.client_billing_row_id
            AND (
              (cbr.is_active = false AND cbr.end_period IS NULL)
              OR sp.id IS NULL
              OR sp.start_date < TO_DATE('01-' || cbr.start_period, 'DD-Mon-YY')
              OR (
                cbr.end_period IS NOT NULL
                AND sp.start_date > TO_DATE('01-' || cbr.end_period, 'DD-Mon-YY')
              )
            )
        )
      )
  `, [updatedBy]);

  return result.rowCount;
};

const cleanupStaleTeamRecurringExpenseRecords = async (dbClient, updatedBy = null) => {
  const result = await dbClient.query(`
    UPDATE expenses e
    SET is_active = false,
        updated_at = NOW(),
        updated_by = COALESCE($1::uuid, e.updated_by)
    WHERE e.is_active = true
      AND e.source_type = 'team-recurring'
      AND e.team_client_allocation_id IS NOT NULL
      AND (
        NOT EXISTS (
          SELECT 1
          FROM team_client_allocations tca
          WHERE tca.id = e.team_client_allocation_id
        )
        OR EXISTS (
          SELECT 1
          FROM team_client_allocations tca
          WHERE tca.id = e.team_client_allocation_id
            AND COALESCE(tca.allocation_percentage, 0) <= 0
        )
        OR EXISTS (
          SELECT 1
          FROM team_client_allocations tca
          JOIN service_periods sp ON sp.id = e.service_period_id
          WHERE tca.id = e.team_client_allocation_id
            AND (
              sp.start_date < TO_DATE('01-' || tca.start_period, 'DD-Mon-YY')
              OR (
                tca.end_period IS NOT NULL
                AND sp.start_date > TO_DATE('01-' || tca.end_period, 'DD-Mon-YY')
              )
            )
        )
      )
  `, [updatedBy]);

  return result.rowCount;
};

const cleanupStaleRecurringExpenseRecords = async (dbClient, updatedBy = null) => {
  const result = await dbClient.query(`
    UPDATE expenses e
    SET is_active = false,
        updated_at = NOW(),
        updated_by = COALESCE($1::uuid, e.updated_by)
    WHERE e.is_active = true
      AND e.source_type = 'recurring'
      AND e.recurring_expense_client_id IS NOT NULL
      AND (
        NOT EXISTS (
          SELECT 1
          FROM recurring_expense_clients rec
          WHERE rec.id = e.recurring_expense_client_id
        )
        OR EXISTS (
          SELECT 1
          FROM recurring_expense_clients rec
          JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
          JOIN recurring_expenses re ON re.id = ret.recurring_expense_id
          WHERE rec.id = e.recurring_expense_client_id
            AND re.is_active = false
        )
        OR EXISTS (
          SELECT 1
          FROM recurring_expense_clients rec
          JOIN recurring_expense_teams ret ON ret.id = rec.recurring_expense_team_id
          JOIN recurring_expenses re ON re.id = ret.recurring_expense_id
          JOIN service_periods sp ON sp.id = e.service_period_id
          WHERE rec.id = e.recurring_expense_client_id
            AND (
              sp.start_date < TO_DATE('01-' || COALESCE(rec.start_period, re.start_period), 'DD-Mon-YY')
              OR (
                COALESCE(rec.end_period, re.end_period) IS NOT NULL
                AND sp.start_date > TO_DATE('01-' || COALESCE(rec.end_period, re.end_period), 'DD-Mon-YY')
              )
            )
        )
      )
  `, [updatedBy]);

  return result.rowCount;
};

const cleanupStaleRecurringSourceRecords = async (dbClient, updatedBy = null) => {
  const recurringRevenues = await cleanupStaleRecurringRevenueRecords(dbClient, updatedBy);
  const teamRecurringExpenses = await cleanupStaleTeamRecurringExpenseRecords(dbClient, updatedBy);
  const recurringExpenses = await cleanupStaleRecurringExpenseRecords(dbClient, updatedBy);

  return {
    recurring_revenues: recurringRevenues,
    team_recurring_expenses: teamRecurringExpenses,
    recurring_expenses: recurringExpenses,
  };
};

module.exports = {
  cleanupStaleRecurringSourceRecords,
  cleanupStaleRecurringRevenueRecords,
  cleanupStaleRecurringExpenseRecords,
  cleanupStaleTeamRecurringExpenseRecords,
  deactivateRecurringRevenuesForBillingRows,
  deactivateTeamRecurringExpensesForAllocations,
};
