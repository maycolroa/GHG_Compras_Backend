import { ColumnaDefinicion } from './columna-definicion.interface';

export interface ShoppingResultadoService {
  id: string;
  nombreTabla: string;
  totalRegistros: number;
  mensaje: string;
  estructuraColumnas: ColumnaDefinicion[];
}