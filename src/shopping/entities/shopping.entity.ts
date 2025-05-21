
import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';
import { ColumnaDefinicion } from '../interface/columna-definicion.interface';

@Entity('shopping')
export class ShoppingEntity {

  @PrimaryGeneratedColumn('uuid')
  id: string;
  
  // Propiedades base necesarias para el servicio
  @Column()
  nombreArchivo: string;

  @Column()
  tipoArchivo: string;

  @CreateDateColumn()
  createdAt: Date;

  @Column()
  nombreTabla: string;

  @Column()
  rutaArchivo: string;

  @Column({ nullable: true })
  usuarioId: string;

  @Column('jsonb', { nullable: true })
  estructuraColumnas: ColumnaDefinicion[];

  @Column({ nullable: true, default: 0 })
  totalRegistros: number;

  @Column({ default: true })
  isActive: boolean;
  
  // Propiedades específicas para el módulo de compras
  @Column({ nullable: true })
  proveedorNombre: string;

  @Column({ nullable: true })
  proveedorNit: string;

  @Column({ nullable: true })
  categoriaProducto: string;

  @Column({ nullable: true })
  departamento: string;

  @Column({ nullable: true })
  numeroOrden: string;

  @Column({ nullable: true })
  telefono: string;

  @Column({ nullable: true })
  correoContacto: string;

  @Column({ nullable: true, type: 'text' })
  responsableComprasId: string;

  @Column({ nullable: true })
  responsableComprasEmail: string;

  @Column({ nullable: true })
  estadoCompra: string;

  @Column({ nullable: true })
  tipoCompra: string;

  @Column({ nullable: true })
  prioridad: string;

  @Column({ nullable: true, type: 'date' })
  fechaOrden: Date;

  @Column({ nullable: true, type: 'numeric', precision: 12, scale: 2 })
  valorTotal: number;

  @Column({ nullable: true, type: 'int' })
  cantidadItems: number;

  @Column({ nullable: true })
  moneda: string;

  @Column({ nullable: true })
  formaPago: string;

  @Column({ nullable: true, type: 'text' })
  direccionEntrega: string;

  @Column({ nullable: true })
  centroCostos: string;

  @Column({ nullable: true })
  solicitante: string;

  @Column({ nullable: true, type: 'date' })
  fechaEntrega: Date;

  @Column({ nullable: true, type: 'date' })
  fechaPago: Date;

  @Column({ nullable: true, type: 'int' })
  diasCredito: number;

  @Column({ nullable: true, type: 'numeric', precision: 12, scale: 2 })
  descuento: number;

  @Column({ nullable: true, type: 'numeric', precision: 12, scale: 2 })
  impuestos: number;

  @Column({ nullable: true, type: 'text' })
  observaciones: string;
}