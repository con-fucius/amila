// Type declarations for dynamically imported react-window
declare module 'react-window' {
  import { ComponentType, ReactNode } from 'react'

  interface FixedSizeListProps {
    height: number
    width: string | number
    itemCount: number
    itemSize: number
    children: ComponentType<{ index: number; style: any }>
    ref?: any
  }

  interface VariableSizeListProps {
    height: number
    width: string | number
    itemCount: number
    itemSize: (index: number) => number
    children: ComponentType<{ index: number; style: any }>
    ref?: any
  }

  export const FixedSizeList: ComponentType<FixedSizeListProps>
  export const VariableSizeList: ComponentType<VariableSizeListProps>
}