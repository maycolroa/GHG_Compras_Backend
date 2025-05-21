import { PartialType } from '@nestjs/mapped-types';
import { CreateShoppingServiceDto } from './create-shopping-service.dto';

export class updateShoppingServiceDtO extends PartialType(
  CreateShoppingServiceDto,
) {}
