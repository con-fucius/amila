// Type declarations for plotly.js-dist-min
declare module 'plotly.js-dist-min' {
  export function newPlot(
    root: HTMLElement,
    data: any[],
    layout?: any,
    config?: any
  ): Promise<any>;
  
  export function purge(root: HTMLElement): void;
  
  export function react(
    root: HTMLElement,
    data: any[],
    layout?: any,
    config?: any
  ): Promise<any>;
  
  export function relayout(root: HTMLElement, update: any): Promise<any>;
  
  export function restyle(root: HTMLElement, update: any, traces?: number[]): Promise<any>;
}
