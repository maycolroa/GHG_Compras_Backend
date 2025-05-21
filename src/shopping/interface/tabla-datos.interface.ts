export interface TablaDatos {
  nombreTabla: string;
  columnas: ColumnaInfo[];
  datos: any[];
  totalRegistros: number;
  page?: number;
  limit?: number;
  totalPages?: number;
}

export interface ColumnaInfo {
  nombre: string;
  tipo: string;
}