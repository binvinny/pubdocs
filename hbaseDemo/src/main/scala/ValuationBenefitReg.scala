package erOutput.Aggregation.Regular.ValuationBenefitReg

case class ValuationBenefitReg(
                                  DataCenter: String,
                                  JobId: Long,
                                  ValuationBenefitKey: Long,
                                  EmployeeKey: Int,
                                  CombinedKey: Int,
                                  executionId: Long,
                                  MajorBreakKeyId: Int,
                                  BenefitTypeKey: Int,
                                  DecrementTypeID: Int,
                                  PayoutBenefitTypeID: Int,
                                  PayoutBenefitTypeName: String,
                                  TrancheTypeName: String,
                                  PriorityCategory: Int,
                                  liabilityType: String,
                                  liabilitySubType: String,
                                  TrancheTypeKey: Int,
                                  DecrementType: String,
                                  BenefitType: String,
                                  Status: String,
                                  DataCount: Int,
                                  AccruedLiability: Int,
                                  NormalCost: Int,
                                  PresentValueBenefits: Int,
                                  expectedBenefitPayments: Int,
                                  WeightedLiability: Int,
                                  IncludeForLiabilityType: String,
                                  OneYearProjectedAccruedLiability: Int,
                                  TermCost: Int
                              )

