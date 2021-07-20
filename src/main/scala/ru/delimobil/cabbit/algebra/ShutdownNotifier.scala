package ru.delimobil.cabbit.algebra

trait ShutdownNotifier[F[_]] {
  def isOpen: F[Boolean]
}
