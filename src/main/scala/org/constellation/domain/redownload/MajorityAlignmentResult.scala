package org.constellation.domain.redownload

sealed trait MajorityAlignmentResult

object AlignedWithMajority extends MajorityAlignmentResult

object MisalignedAtSameHeight extends MajorityAlignmentResult

object BelowPastRedownloadInterval extends MajorityAlignmentResult
object BelowButMisalignedBeforeRedownloadIntervalAnyway extends MajorityAlignmentResult

object AbovePastRedownloadInterval extends MajorityAlignmentResult
object AboveButMisalignedBeforeRedownloadIntervalAnyway extends MajorityAlignmentResult
