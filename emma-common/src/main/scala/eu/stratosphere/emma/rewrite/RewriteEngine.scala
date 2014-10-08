package eu.stratosphere.emma.rewrite

trait RewriteEngine {

  type Expression

  abstract class Rule {

    type RuleMatch

    protected def bind(e: Expression): Option[RuleMatch]

    protected def guard(m: RuleMatch): Boolean

    protected def fire(m: RuleMatch): Expression

    final def apply(e: Expression): Option[Expression] = for (m <- bind(e); if guard(m)) yield fire(m)
  }

}
