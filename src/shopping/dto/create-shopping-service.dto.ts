import {
  IsString,
  IsNotEmpty,
  IsOptional,
  IsNumber,
  IsDateString,
} from 'class-validator';

export class CreateShoppingServiceDto {
  @IsString()
  @IsNotEmpty()
  nombreTabla: string;

  @IsString()
  @IsOptional()
  usuarioId?: string;

  @IsString()
  @IsOptional()
  estado?: string;

  @IsNumber()
  @IsOptional()
  cantidad_pedida?: number;

  @IsString()
  @IsOptional()
  proveedor?: string;

  @IsString()
  @IsOptional()
  categoria?: string;

  @IsDateString()
  @IsOptional()
  fecha_entrega?: string;
}